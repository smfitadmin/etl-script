<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\Validator;
use Illuminate\Database\QueryException;
use JsonMachine\Items;
use Carbon\Carbon;

class ImportRemAdvice extends Command
{
    protected $signature = 'remittance-report:import
        {path : Path to JSON file}
        {--table=rm_detail_report : DB table name}
        {--connection= : DB connection name (override default)}
        {--pointer=/transactions : JSON pointer (root->transactions)}
        {--dry-run : Validate only (no DB write)}';

    protected $description = 'Insert-only importer for CPALL remittance transactions (Thai keys) with low memory.';

    public function handle(): int
    {
        $path      = $this->argument('path');
        $table     = $this->option('table');
        $connName  = $this->option('connection') ?: config('database.default');
        $pointer   = $this->option('pointer') ?? '/transactions';
        $dryRun    = (bool) $this->option('dry-run');

        if (!is_readable($path)) {
            $this->error("File not readable: {$path}");
            return self::FAILURE;
        }

        DB::connection($connName)->disableQueryLog();
        @set_time_limit(0);

        $this->info("Importing: {$path}");
        $this->line("Table: {$table}");
        $this->line("Connection: {$connName}");
        $this->line("Pointer: " . ($pointer === '' ? '(none)' : $pointer));
        $this->line("Dry-run: " . ($dryRun ? 'YES' : 'NO'));

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

        // เตรียม stream JSON
        try {
            $items = ($pointer === '' ? Items::fromFile($path) : Items::fromFile($path, ['pointer' => $pointer]));
        } catch (\Throwable $e) {
            $this->error("Failed to open/parse JSON: " . $e->getMessage());
            return self::FAILURE;
        }

        // map คีย์ภาษาไทย -> คอลัมน์ DB
        $thaiToDb = [
            'วันที่'               => 'invoice_date',
            'วันที่เอกสาร'         => 'report_date',
            'วันที่จ่ายเงิน'       => 'payment_date',
            'รหัสผู้ขาย'           => 'supplier_code',
            'รหัสสาขา'             => 'branch_code',
            'ประเภทเอกสาร'         => 'doc_type',
            'เลขที่เอกสาร'         => 'doc_no',
            'เลขที่เอกสารอ้างอิง'   => 'doc_ref_no',
            'จำนวน'                 => 'rm_amount',
            // 'หน้า' ไม่ใช้
        ];

        // กฎ validate (หลังแปลง)
        $rules = [
            'report_date'   => 'nullable|date_format:Y-m-d',
            'payment_date'  => 'nullable|date_format:Y-m-d',
            'invoice_date'  => 'nullable|date_format:Y-m-d',
            'supplier_code' => 'nullable|string',
            'branch_code'   => 'nullable|string',
            'doc_type'      => 'nullable|string|max:20',
            'doc_no'        => 'nullable|string',
            'doc_ref_no'    => 'nullable|string',
            'rm_amount'     => 'nullable|numeric',
        ];

        $total = 0;
        $inserted = 0;
        $failed = 0;

        $nowStr = now()->format('Y-m-d H:i:s');
        $dbTbl  = DB::connection($connName)->table($table);

        $bar = $this->output->createProgressBar();
        $bar->start();

        foreach ($items as $raw) {
            $total++;

            if ($raw instanceof \stdClass) $raw = (array) $raw;
            if (!is_array($raw)) {
                $failed++;
                $bar->advance();
                continue;
            }

            // แม็พชื่อคีย์
            $row = [];
            foreach ($thaiToDb as $th => $dbKey) {
                if (array_key_exists($th, $raw)) {
                    $row[$dbKey] = $raw[$th];
                }
            }

            // แปลงวันที่
            foreach (['report_date', 'payment_date', 'invoice_date'] as $dk) {
                $row[$dk] = $this->toYmdOrNull($row[$dk] ?? null);
            }
            // autofill report_date จาก invoice_date เมื่อว่าง
            if (empty($row['report_date']) && !empty($row['invoice_date'])) {
                $row['report_date'] = $row['invoice_date'];
            }

            // แปลงจำนวนเงิน
            if (array_key_exists('rm_amount', $row)) {
                $row['rm_amount'] = $this->toNumberOrNull($row['rm_amount']);
            }

            // จำกัดเฉพาะคีย์ที่มีในตาราง และกัน array/object
            $row = array_intersect_key($row, $columnsFlip);
            foreach ($row as $k => $v) {
                if (is_array($v) || is_object($v)) unset($row[$k]);
            }

            // เติม timestamps ถ้ามีคอลัมน์
            if ($hasTimestamps) {
                $row['created_at'] = $nowStr;
                $row['updated_at'] = $nowStr;
            }

            // validate หลังแปลง
            $v = Validator::make($row, $rules);
            if ($v->fails()) {
                $failed++;
                $bar->advance();
                continue;
            }
            $row = $v->validated() + ($hasTimestamps ? ['created_at' => $nowStr, 'updated_at' => $nowStr] : []);

            if ($dryRun) {
                $inserted++;
                $bar->advance();
                if ($total % 1000 === 0) gc_collect_cycles();
                continue;
            }

            // INSERT-ONLY (ไม่มี upsert/ไม่มี key check)
            try {
                $dbTbl->insert($row);
                $inserted++;
            } catch (QueryException $e) {
                // ถ้ามี UNIQUE index ใน DB จะล้มที่นี่
                $failed++;
                $this->output->writeln("");
                $this->error("Row {$total} failed: " . $e->getMessage());
            } catch (\Throwable $e) {
                $failed++;
                $this->output->writeln("");
                $this->error("Row {$total} failed: " . $e->getMessage());
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
        if ($dryRun) $this->warn("Dry-run mode: no database changes were made.");

        return self::SUCCESS;
    }

    protected function toYmdOrNull(?string $s): ?string
    {
        if ($s === null) return null;
        $s = trim($s);
        if ($s === '') return null;
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $s)) return $s;

        $fmts = [
            'd-m-Y',
            'd/m/Y',
            'd.m.Y',
            'Y/m/d',
            'Y.m.d',
            'm/d/Y',
            'm-d-Y',
            'd M Y',
            'd M Y H:i',
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

    protected function toNumberOrNull($v): ?float
    {
        if ($v === null) return null;
        if (is_float($v) || is_int($v)) return (float) $v;
        if (!is_string($v)) return null;

        $s = trim($v);
        $s = preg_replace('/[^\d\.\,\-]/u', '', $s);
        $s = str_replace(',', '', $s);
        if ($s === '' || $s === '.' || $s === '-') return null;

        if (substr_count($s, '.') > 1) {
            $lastDot = strrpos($s, '.');
            $s = str_replace('.', '', $s);
            $s = substr_replace($s, '.', $lastDot, 0);
        }

        if (!is_numeric($s)) return null;
        return (float) $s;
    }
}
