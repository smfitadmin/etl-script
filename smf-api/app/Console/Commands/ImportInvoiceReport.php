<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\Validator;
use Illuminate\Database\QueryException;
use JsonMachine\Items;
use Carbon\Carbon;

class ImportInvoiceReport extends Command
{
    protected $signature = 'invoice-report:import
        {path : Path to JSON file}
        {--table=invoice_detail_report : DB table name}
        {--connection= : DB connection name (override default)}
        {--pointer=/records : JSON pointer when root is an object (e.g. /records); leave empty for array root}
        {--dry-run : Validate only (no DB write)}
        {--max-log-errors=20 : Max error samples to print}
        {--dump-fail= : Write failed rows with reasons to this JSON file}
        {--no-validate : Skip all data validation and let DB enforce constraints}';

    protected $description = 'Insert-only importer for Invoice detail report (low-memory streaming, with diagnostics).';

    // นับจำนวนที่ถูกแก้กรณี YYYY-DD-MM -> YYYY-MM-DD สำหรับ invoice_received_date
    protected int $fixedReceivedSwap = 0;

    // นับจำนวนแถวที่ถูกมองเป็น header แล้วข้าม
    protected int $skippedHeaderRows = 0;

    public function handle(): int
    {
        $path       = $this->argument('path');
        $table      = $this->option('table');
        $connName   = $this->option('connection') ?: config('database.default');
        $pointer    = $this->option('pointer'); // default /records
        $dryRun     = (bool) $this->option('dry-run');
        $maxLog     = (int) $this->option('max-log-errors');
        $dumpFile   = $this->option('dump-fail');
        $noValidate = (bool) $this->option('no-validate');

        if (!is_readable($path)) {
            $this->error("File not readable: {$path}");
            return self::FAILURE;
        }

        DB::connection($connName)->disableQueryLog();
        @set_time_limit(0);

        $this->info("Importing: {$path}");
        $this->line("Table: {$table}");
        $this->line("Connection: {$connName}");
        $this->line("Pointer: " . ($pointer === null ? '(none, array root)' : ($pointer === '' ? '(none)' : $pointer)));
        $this->line("Dry-run: " . ($dryRun ? 'YES' : 'NO'));

        // อ่าน schema ตาราง
        try {
            $columns = Schema::connection($connName)->getColumnListing($table);
        } catch (\Throwable $e) {
            $this->error("Cannot read schema for table '{$table}': " . $e->getMessage());
            return self::FAILURE;
        }
        $columnsFlip   = array_flip($columns);
        $hasCreatedAt  = in_array('created_at', $columns, true);
        $hasUpdatedAt  = in_array('updated_at', $columns, true);
        $hasTimestamps = $hasCreatedAt && $hasUpdatedAt;
        $this->line('Timestamps columns: ' . ($hasTimestamps ? 'present' : 'absent (skipping)'));

        // เปิด JSON stream
        try {
            $items = ($pointer === null || $pointer === '')
                ? Items::fromFile($path)                             // array root
                : Items::fromFile($path, ['pointer' => $pointer]);   // object root with pointer (e.g. /records)
        } catch (\Throwable $e) {
            $this->error("Failed to open/parse JSON: " . $e->getMessage());
            return self::FAILURE;
        }

        // map key อินพุต -> คอลัมน์ DB
        $map = [
            'Invoice No.'           => 'invoice_no',
            'Supplier Code'         => 'supplier_code',
            'Supplier Name'         => 'supplier_name',
            'Invoice Date'          => 'invoice_date',           // DATE
            'Invoice Received Date' => 'invoice_received_date',  // DATETIME
            'Related Document'      => 'po_no',
            'Amount'                => 'amount',
            'Status'                => 'status',
            // 'No' ไม่เก็บ
        ];

        // กฎ validate หลัง normalize (ปล่อย invoice_received_date เป็น string เพราะ normalize เอง)
        $rules = [
            'invoice_no'            => 'nullable|string',
            'supplier_code'         => 'nullable|string',
            'supplier_name'         => 'nullable|string',
            'invoice_date'          => 'nullable|date_format:Y-m-d',
            'invoice_received_date' => 'nullable|string',
            'po_no'                 => 'nullable|string',
            'amount'                => 'nullable|numeric',
            'status'                => 'nullable|string|max:50',
        ];

        // ตัวนับ/diag
        $total = 0;
        $inserted = 0;
        $failed = 0;
        $failValidation = 0;
        $failDupKey     = 0;
        $failDbOther    = 0;

        $errorSamples = [];  // เก็บตัวอย่าง error
        $failedRows   = [];  // สำหรับเขียนไฟล์ dump

        $nowStr = now()->format('Y-m-d H:i:s');
        $dbTbl  = DB::connection($connName)->table($table);

        $bar = $this->output->createProgressBar();
        $bar->start();

        foreach ($items as $raw) {
            $total++;

            if ($raw instanceof \stdClass) $raw = (array) $raw;
            if (!is_array($raw)) {
                $failed++;
                $failValidation++;
                $this->captureError($errorSamples, $maxLog, $failedRows, $dumpFile, $total, 'validation', 'Non-array row', null, $raw);
                $bar->advance();
                continue;
            }

            // map keys
            $row = [];
            foreach ($map as $src => $dst) {
                if (array_key_exists($src, $raw)) $row[$dst] = $raw[$src];
            }

            // normalize
            $row['invoice_date']          = $this->toYmdOrNull($row['invoice_date'] ?? null);
            $row['invoice_received_date'] = $this->toYmdHisOrNull_Received($row['invoice_received_date'] ?? null); // robust
            if (array_key_exists('amount', $row)) {
                $row['amount'] = $this->toNumberOrNull($row['amount']);
            }

            // จำกัดเฉพาะคอลัมน์ที่มีจริง และกัน array/object ออก
            $row = array_intersect_key($row, $columnsFlip);
            foreach ($row as $k => $v) {
                if (is_array($v) || is_object($v)) unset($row[$k]);
            }

            // ข้ามแถวที่เป็น header-like (เช่น "Invoice No.", "Supplier Code", ...)
            if ($this->looksLikeHeaderRow($row)) {
                $this->skippedHeaderRows++;
                $this->captureError($errorSamples, $maxLog, $failedRows, $dumpFile, $total, 'skipped_header', 'Detected header row', null, $row);
                $bar->advance();
                continue;
            }

            // เติม timestamps ถ้ามีคอลัมน์
            if ($hasTimestamps) {
                $row['created_at'] = $nowStr;
                $row['updated_at'] = $nowStr;
            }

            // Validate หลัง normalize (ถ้าไม่ปิดด้วย --no-validate)
            if (!$noValidate) {
                $v = Validator::make($row, $rules);
                if ($v->fails()) {
                    $failed++;
                    $failValidation++;
                    $errorsArr = $v->errors()->toArray();

                    if (count($errorSamples) < $maxLog) {
                        $this->output->writeln("");
                        $this->error("Row {$total} validation errors:");
                        foreach ($errorsArr as $field => $messages) {
                            $msg = is_array($messages) ? implode('; ', array_slice($messages, 0, 2)) : (string)$messages;
                            $this->line("  - {$field}: {$msg}");
                        }
                    }

                    $this->captureError($errorSamples, $maxLog, $failedRows, $dumpFile, $total, 'validation', $errorsArr, null, $row);
                    $bar->advance();
                    continue;
                }
                $row = $v->validated() + ($hasTimestamps ? ['created_at' => $nowStr, 'updated_at' => $nowStr] : []);
            }

            if ($dryRun) {
                $inserted++;
                $bar->advance();
                if ($total % 1000 === 0) gc_collect_cycles();
                continue;
            }

            // INSERT-ONLY
            try {
                $dbTbl->insert($row);
                $inserted++;
            } catch (QueryException $e) {
                $failed++;

                $msg = $e->getMessage();
                $sqlstate = $e->getCode(); // 23000 รวมเคสหลายแบบ
                $isDuplicate = str_contains($msg, 'Duplicate entry');
                $isNotNull   = str_contains($msg, 'cannot be null') || str_contains($msg, "doesn't have a default value");

                if ($isDuplicate) {
                    $failDupKey++;
                } else {
                    $failDbOther++; // รวม NOT NULL และ error อื่น ๆ
                }

                $reason = $isDuplicate ? 'duplicate_key' : ($isNotNull ? 'not_null_violation' : 'db_error');

                $this->captureError(
                    $errorSamples,
                    $maxLog,
                    $failedRows,
                    $dumpFile,
                    $total,
                    $reason,
                    $msg,
                    $sqlstate,
                    $row
                );

                if (count($errorSamples) <= $maxLog) {
                    $this->output->writeln("");
                    $this->error("Row {$total} {$reason}: " . $msg);
                }
            } catch (\Throwable $e) {
                $failed++;
                $failDbOther++;
                $this->captureError(
                    $errorSamples,
                    $maxLog,
                    $failedRows,
                    $dumpFile,
                    $total,
                    'db_error',
                    $e->getMessage(),
                    null,
                    $row
                );
                if (count($errorSamples) <= $maxLog) {
                    $this->output->writeln("");
                    $this->error("Row {$total} db_error: " . $e->getMessage());
                }
            }

            $bar->advance();
            if ($total % 1000 === 0) gc_collect_cycles();
        }

        $bar->finish();
        $this->newLine(2);
        $this->info("Done.");
        $this->line("Total read: {$total}");
        $this->line("Inserted:   {$inserted}");
        $this->line("Failed:     {$failed}");
        $this->line("Failed (validation):     {$failValidation}");
        $this->line("Failed (duplicate key):  {$failDupKey}");
        $this->line("Failed (other DB error): {$failDbOther}");
        $this->line("Fixed invoice_received_date (day-month swapped): {$this->fixedReceivedSwap}");
        $this->line("Skipped header-like rows: {$this->skippedHeaderRows}");
        if ($dryRun) $this->warn("Dry-run mode: no database changes were made.");

        // dump ไฟล์ failed rows ถ้าระบุ
        if ($dumpFile) {
            try {
                file_put_contents($dumpFile, json_encode($failedRows, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT));
                $this->info("Failed rows written to: {$dumpFile}");
            } catch (\Throwable $e) {
                $this->warn("Could not write dump-fail file: " . $e->getMessage());
            }
        }

        // แสดงสารบัญตัวอย่าง error
        if (!empty($errorSamples)) {
            $this->newLine();
            $this->warn("Error samples (showing up to {$maxLog}):");
            foreach ($errorSamples as $es) {
                $line = "- Row {$es['row']} :: {$es['reason']}";
                if (!empty($es['sqlstate'])) $line .= " (SQLSTATE {$es['sqlstate']})";
                $this->line($line);
            }
        }

        return self::SUCCESS;
    }

    /** ตรวจว่า row ดูเป็น header ของตารางหรือไม่ (เช่นค่าตรงกับหัวคอลัมน์) */
    protected function looksLikeHeaderRow(array $row): bool
    {
        $headers = [
            'invoice_no'            => ['Invoice No', 'Invoice No.'],
            'supplier_code'         => ['Supplier Code'],
            'supplier_name'         => ['Supplier Name'],
            'invoice_date'          => ['Invoice Date'],
            'invoice_received_date' => ['Invoice Received Date'],
            'po_no'                 => ['Related Document', 'PO No', 'PO No.'],
            'amount'                => ['Amount', 'Amount (THB)', 'Amount Include VAT'],
            'status'                => ['Status'],
        ];
        foreach ($headers as $key => $candidates) {
            if (!array_key_exists($key, $row)) continue;
            $val = trim((string)$row[$key]);
            foreach ($candidates as $h) {
                if (strcasecmp($val, $h) === 0) return true;
            }
        }
        return false;
    }

    /** เก็บตัวอย่างความผิดพลาด (และลงไฟล์ dump ถ้าระบุ) */
    protected function captureError(array &$samples, int $max, array &$dump, ?string $dumpFile, int $rowNum, string $reason, $detail, $sqlstate = null, $data = null): void
    {
        $entry = [
            'row'      => $rowNum,
            'reason'   => $reason,
            'detail'   => $detail,
            'sqlstate' => $sqlstate,
            'data'     => $data,
        ];
        if (count($samples) < $max) $samples[] = $entry;
        if ($dumpFile) $dump[] = $entry;
    }

    /** แปลงเป็น Y-m-d สำหรับฟิลด์ date-only; ตัด token แปลก ๆ เป็น null */
    protected function toYmdOrNull(?string $s): ?string
    {
        if ($s === null) return null;
        $s = trim((string)$s);
        if ($s === '' || $s === '-' || $s === '?' || strcasecmp($s, 'null') === 0 || strcasecmp($s, 'n/a') === 0) {
            return null;
        }
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $s)) return $s;

        $fmts = [
            'Y-m-d H:i:s',
            'Y/m/d H:i:s',
            'd-m-Y',
            'd/m/Y',
            'd.m.Y',
            'Y/m/d',
            'Y.m.d',
            'm/d/Y',
            'm-d-Y',
            'd M Y',
            'd M Y H:i',
            'Y-m-d\TH:i:s',
        ];
        foreach ($fmts as $f) {
            try {
                $dt = Carbon::createFromFormat($f, $s);
                if ($dt !== false) return $dt->format('Y-m-d');
            } catch (\Throwable $e) {
            }
        }
        try {
            return Carbon::parse($s)->format('Y-m-d');
        } catch (\Throwable $e) {
            return null;
        }
    }

    /** แปลงเป็น Y-m-d H:i:s สำหรับ invoice_received_date; ตรวจจริง + auto-fix YYYY-DD-MM + ตัด token แปลก ๆ */
    protected function toYmdHisOrNull_Received(?string $s): ?string
    {
        if ($s === null) return null;
        $s = trim((string)$s);
        if ($s === '' || $s === '-' || $s === '?' || strcasecmp($s, 'null') === 0 || strcasecmp($s, 'n/a') === 0) {
            return null;
        }

        // (1) ถ้าหน้าตาเป็น Y-m-d H:i:s ให้ตรวจความถูกต้องจริง และสลับวัน/เดือนถ้าจำเป็น
        if (preg_match('/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/', $s, $m)) {
            $y = (int)$m[1];
            $mth = (int)$m[2];
            $day = (int)$m[3];
            $H = (int)$m[4];
            $i = (int)$m[5];
            $S = (int)$m[6];

            // เคส YYYY-DD-MM -> สลับ
            if ($mth > 12 && $day >= 1 && $day <= 12) {
                try {
                    $dt = Carbon::create($y, $day, $mth, $H, $i, $S);
                    if ($dt !== false) {
                        $this->fixedReceivedSwap++;
                        return $dt->format('Y-m-d H:i:s');
                    }
                } catch (\Throwable $e) {
                }
            }

            if (checkdate($mth, $day, $y) && $H <= 23 && $i <= 59 && $S <= 59) {
                return sprintf('%04d-%02d-%02d %02d:%02d:%02d', $y, $mth, $day, $H, $i, $S);
            }
            // ถ้าไม่ valid จริง ให้ไหลไป parse แบบอื่น
        }

        // (2) รูปแบบที่พบบ่อย (ISO/ไม่มีวินาที/มี millis/มี timezone)
        $formats = [
            'Y-m-d\TH:i:sP',
            'Y-m-d\TH:i:s.uP',
            'Y-m-d\TH:iP',
            'Y-m-d\TH:i:s',
            'Y-m-d\TH:i:s.u',
            'Y-m-d\TH:i',
            'Y-m-d H:i:s',
            'Y-m-d H:i:s.u',
            'Y-m-d H:i',
            'Y/m/d H:i:s',
            'Y/m/d H:i',
            'd/m/Y H:i:s',
            'd/m/Y H:i',
            'd-m-Y H:i:s',
            'd-m-Y H:i',
            'Y-m-d',
            'Y/m/d',
            'd/m/Y',
            'd-m-Y',
        ];
        foreach ($formats as $f) {
            try {
                $dt = Carbon::createFromFormat($f, $s);
                if ($dt !== false) return $dt->format('Y-m-d H:i:s');
            } catch (\Throwable $e) {
            }
        }

        // (3) Regex จับ YYYY-DD-MM [ ]HH:MM[:SS] แล้วสลับ
        if (preg_match('/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/', $s, $m)) {
            $y = (int)$m[1];
            $mid = (int)$m[2];
            $last = (int)$m[3];
            $H = (int)$m[4];
            $i = (int)$m[5];
            $S = isset($m[6]) ? (int)$m[6] : 0;

            if ($mid > 12 && $last >= 1 && $last <= 12) {
                try {
                    $dt = Carbon::create($y, $last, $mid, $H, $i, $S);
                    if ($dt !== false) {
                        $this->fixedReceivedSwap++;
                        return $dt->format('Y-m-d H:i:s');
                    }
                } catch (\Throwable $e) {
                }
            }

            // ถ้าไม่มีวินาที ให้เติม 00
            if (!isset($m[6])) {
                try {
                    $dt = Carbon::create($y, $mid, $last, $H, $i, 0);
                    if ($dt !== false) return $dt->format('Y-m-d H:i:s');
                } catch (\Throwable $e) {
                }
            }
        }

        // (4) สุดท้าย: parser อิสระ (Z, +07:00, millis)
        try {
            return Carbon::parse($s)->format('Y-m-d H:i:s');
        } catch (\Throwable $e) {
            return null;
        }
    }

    /** แปลงสตริงตัวเลขให้เป็น float; ตัดสัญลักษณ์/คอมมา/ช่องว่างที่ไม่จำเป็น */
    protected function toNumberOrNull($v): ?float
    {
        if ($v === null) return null;
        if (is_float($v) || is_int($v)) return (float) $v;
        if (!is_string($v)) return null;

        $s = trim($v);
        // ตัดทุกอย่างที่ไม่ใช่เลข, จุด, คอมมา, ลบ
        $s = preg_replace('/[^\d\.\,\-]/u', '', $s);
        // ตัดคอมมา
        $s = str_replace(',', '', $s);
        if ($s === '' || $s === '.' || $s === '-') return null;

        // ถ้ามีจุดหลายตัว เหลือจุดท้ายเป็นทศนิยม
        if (substr_count($s, '.') > 1) {
            $lastDot = strrpos($s, '.');
            $s = str_replace('.', '', $s);
            $s = substr_replace($s, '.', $lastDot, 0);
        }

        if (!is_numeric($s)) return null;
        return (float) $s;
    }
}
