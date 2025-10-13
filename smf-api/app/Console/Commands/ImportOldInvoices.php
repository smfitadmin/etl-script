<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\Validator;
use Illuminate\Database\QueryException;
use JsonMachine\Items;
use Carbon\Carbon;

class ImportOldInvoices extends Command
{
    protected $signature = 'invoices:import
        {path : Path to JSON file (root = array of objects)}
        {--table=gec_inv_2025 : DB table name}
        {--connection= : DB connection name (override default)}
        {--pointer= : JSON pointer when root is an object, e.g. /items}
        {--match= : Comma-separated keys for per-row upsert (e.g. invoice_no,buyer_code)}
        {--dry-run : Validate only (no DB write)}';

    protected $description = 'Stream-import invoice JSON and insert one row at a time (very low memory).';

    public function handle(): int
    {
        $path      = $this->argument('path');
        $table     = $this->option('table');
        $connName  = $this->option('connection') ?: config('database.default');
        $pointer   = $this->option('pointer');
        $dryRun    = (bool) $this->option('dry-run');
        $matchOpt  = $this->option('match');
        $matchKeys = $matchOpt ? array_values(array_filter(array_map('trim', explode(',', $matchOpt)))) : [];

        if (!is_readable($path)) {
            $this->error("File not readable: {$path}");
            return self::FAILURE;
        }

        // ลดการใช้หน่วยความจำ
        DB::connection($connName)->disableQueryLog();
        @set_time_limit(0);

        $this->info("Importing: {$path}");
        $this->line("Table: {$table}");
        $this->line("Connection: {$connName}");
        $this->line("Pointer: " . ($pointer ? $pointer : '(none)'));
        $this->line("Dry-run: " . ($dryRun ? 'YES' : 'NO'));
        if ($matchKeys) $this->line("Per-row upsert keys: " . implode(', ', $matchKeys));

        // ตรวจ schema ว่ามีคอลัมน์ครบ
        try {
            $columns = Schema::connection($connName)->getColumnListing($table);
        } catch (\Throwable $e) {
            $this->error("Cannot read schema for table '{$table}': " . $e->getMessage());
            return self::FAILURE;
        }
        $need = [
            'invoice_no',
            'invoice_date',
            'po_no',
            'po_date',
            'supplier_code',
            'buyer_code',
            'amount_excl_vat',
            'vat_amount',
            'amount_incl_vat',
            'created_at',
            'updated_at'
        ];
        $missing = array_diff($need, $columns);
        if ($missing) {
            $this->error("Table '{$table}' missing columns: " . implode(', ', $missing));
            return self::FAILURE;
        }
        $columnsFlip = array_flip($columns);

        // เตรียมสตรีม JSON
        try {
            $items = $pointer ? Items::fromFile($path, ['pointer' => $pointer])
                : Items::fromFile($path);
        } catch (\Throwable $e) {
            $this->error("Failed to open/parse JSON: " . $e->getMessage());
            return self::FAILURE;
        }

        // ตรวจว่า match keys (ถ้ามี) อยู่ในคอลัมน์จริง
        foreach ($matchKeys as $k) {
            if (!isset($columnsFlip[$k])) {
                $this->error("Match key '{$k}' not found in table '{$table}'.");
                return self::FAILURE;
            }
        }

        // rules ตรงกับ JSON ที่คุณให้มา
        $rules = [
            'invoice_no'       => 'nullable|string',
            'invoice_date'     => 'nullable|string', // จะ convert เป็น Y-m-d
            'po_no'            => 'nullable|string',
            'po_date'          => 'nullable|string',
            'supplier_code'    => 'nullable|string',
            'buyer_code'       => 'nullable|string',
            'amount_excl_vat'  => 'nullable|numeric',
            'vat_amount'       => 'nullable|numeric',
            'amount_incl_vat'  => 'nullable|numeric',
        ];

        $total    = 0;
        $inserted = 0;
        $failed   = 0;
        $nowStr   = now()->format('Y-m-d H:i:s');
        $dbTbl    = DB::connection($connName)->table($table);

        $bar = $this->output->createProgressBar();
        $bar->start();

        foreach ($items as $row) {
            $total++;

            if ($row instanceof \stdClass) $row = (array) $row;
            if (!is_array($row)) {
                $failed++;
                $bar->advance();
                continue;
            }

            // ใช้เฉพาะคีย์ที่ต้องการ
            $row = array_intersect_key($row, array_flip([
                'invoice_no',
                'invoice_date',
                'po_no',
                'po_date',
                'supplier_code',
                'buyer_code',
                'amount_excl_vat',
                'vat_amount',
                'amount_incl_vat'
            ]));

            // validate
            $v = Validator::make($row, $rules);
            if ($v->fails()) {
                $failed++;
                $bar->advance();
                continue;
            }
            $row = $v->validated();

            // วันที่: รองรับ dd-mm-YYYY (เช่น 01-12-2023) และรูปแบบทั่วไป
            $row['invoice_date'] = $this->toYmdOrNull($row['invoice_date'] ?? null);
            $row['po_date']      = $this->toYmdOrNull($row['po_date'] ?? null);

            // ตัวเลข
            foreach (['amount_excl_vat', 'vat_amount', 'amount_incl_vat'] as $nc) {
                if (array_key_exists($nc, $row)) {
                    $val = $row[$nc];
                    if ($val === '' || $val === null) $row[$nc] = null;
                    else $row[$nc] = (float) $val;
                }
            }

            // timestamps เป็นสตริงเดียว (ลด overhead)
            $row['created_at'] = $nowStr;
            $row['updated_at'] = $nowStr;

            // กันคีย์แปลก ๆ ที่ไม่อยู่ในตาราง และกัน array/object
            $row = array_intersect_key($row, $columnsFlip);
            foreach ($row as $k => $v) {
                if (is_array($v) || is_object($v)) unset($row[$k]);
            }

            if ($dryRun) {
                // โหมดทดสอบ: ไม่นับเป็น fail แต่ก็ไม่เขียน DB
                $inserted++;
                $bar->advance();
                if ($total % 1000 === 0) gc_collect_cycles();
                continue;
            }

            // === Insert ทีละแถว ===
            try {
                if ($matchKeys) {
                    // per-row upsert โดยใช้ match keys ที่กำหนด
                    $match = array_intersect_key($row, array_flip($matchKeys));
                    $dbTbl->updateOrInsert($match, $row);
                } else {
                    // insert ธรรมดา; ถ้าตารางมี unique อาจชน duplicate — จับแล้วข้าม
                    $dbTbl->insert($row);
                }
                $inserted++;
            } catch (QueryException $e) {
                // ถ้าอยากเงียบกับ duplicate key (SQLSTATE 23000) ให้ข้าม
                if ($e->getCode() === '23000') {
                    // duplicate -> ข้าม
                } else {
                    // อื่น ๆ นับเป็น fail และพิมพ์สั้น ๆ เพื่อดีบัก
                    $failed++;
                    $this->output->writeln("");
                    $this->error("Row {$total} failed: " . $e->getMessage());
                }
            } catch (\Throwable $e) {
                $failed++;
                $this->output->writeln("");
                $this->error("Row {$total} failed: " . $e->getMessage());
            }

            $bar->advance();
            if ($total % 1000 === 0) gc_collect_cycles(); // เก็บกวาดหน่วยความจำเป็นระยะ
        }

        $bar->finish();
        $this->newLine(2);
        $this->info("Done.");
        $this->line("Total read: {$total}");
        $this->line("Inserted:   {$inserted}");
        $this->line("Failed:     {$failed}");
        if ($dryRun) $this->warn("Dry-run mode: no database changes were made.");

        return self::SUCCESS;
    }

    // แปลงวันที่ให้เป็น YYYY-MM-DD; รองรับ dd-mm-YYYY เป็นพิเศษ
    protected function toYmdOrNull(?string $s): ?string
    {
        if (!$s) return null;
        $s = trim($s);
        if ($s === '') return null;

        // already Y-m-d
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $s)) return $s;

        // 1) pre-normalize: dd-mm-yy / dd/mm/yy / dd.mm.yy  => force 20yy
        if (preg_match('/^(?<d>\d{1,2})[\/\.-](?<m>\d{1,2})[\/\.-](?<y>\d{2})$/', $s, $m)) {
            $yy = (int)$m['y'];
            $yyyy = 2000 + $yy; // บังคับเป็น ค.ศ. 20xx เสมอ
            $d = str_pad($m['d'], 2, '0', STR_PAD_LEFT);
            $mo = str_pad($m['m'], 2, '0', STR_PAD_LEFT);
            return sprintf('%04d-%02d-%02d', $yyyy, (int)$mo, (int)$d);
        }

        // 2) try known formats
        $fmts = [
            'd-m-Y',
            'd/m/Y',
            'd.m.Y',
            'Y/m/d',
            'Y.m.d',
            'm/d/Y',
            'm-d-Y',
            'd-m-y',
            'd/m/y',
            'd.m.y',   // ปี 2 หลัก
        ];

        foreach ($fmts as $f) {
            try {
                $dt = \Carbon\Carbon::createFromFormat($f, $s);
                if ($dt !== false) {
                    // ถ้าเป็นฟอร์แมตปี 2 หลัก บังคับเพิ่ม 2000
                    if (str_contains($f, 'y') && $dt->year < 100) {
                        $dt = $dt->setYear(2000 + $dt->year); // << สำคัญ: ใช้ setYear()
                    }
                    return $dt->format('Y-m-d');
                }
            } catch (\Throwable $e) {
                // ignore and try next
            }
        }

        // 3) fallback parse (อาจเดาไม่ตรง จึงไม่บังคับ 2 หลักที่นี่)
        try {
            return \Carbon\Carbon::parse($s)->format('Y-m-d');
        } catch (\Throwable $e) {
            return null;
        }
    }
}
