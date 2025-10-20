<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\Validator;
use Illuminate\Database\QueryException;
use JsonMachine\Items;
use Carbon\Carbon;

class ImportSaleInvoiceReport extends Command
{
    protected $signature = 'sale-invoice:import
        {path : Path to JSON file}
        {--table=sale_invoice_report : DB table name}
        {--connection= : DB connection name (override default)}
        {--dry-run : Validate only (no DB write)}';

    protected $description = 'Insert-only importer for "รายงานการขายสินค้า - แยกตาม Invoice" (Thai keys) with root-array JSON.';

    public function handle(): int
    {
        $path      = $this->argument('path');
        $table     = $this->option('table');
        $connName  = $this->option('connection') ?: config('database.default');
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
        $this->line("JSON root: array [] (no pointer)");
        $this->line("Dry-run: " . ($dryRun ? 'YES' : 'NO'));

        // อ่าน schema
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

        // stream JSON (root array)
        try {
            $items = Items::fromFile($path);
        } catch (\Throwable $e) {
            $this->error("Failed to open/parse JSON: " . $e->getMessage());
            return self::FAILURE;
        }

        // map คีย์ภาษาไทย -> คอลัมน์ DB
        $thaiToDb = [
            'เลขที่เอกสาร'       => 'doc_no',
            'Invoice no.'      => 'invoice_no',
            'วันที่เอกสาร'       => 'invoice_date',
            'PO no.'           => 'po_no',
            'CN. Ref. Doc.'    => 'cn_ref_doc',
            'Assignment'       => 'assignment',
            'จำนวนเงิน'         => 'amount',
            'ภาษี'             => 'vat',
            'จำนวนเงินสุทธิ'     => 'net_amount',
            'start_round_date' => 'start_round_date',
            'end_round_date'   => 'end_round_date',
            'supplier_name'    => 'supplier_name',
            'supplier_num'     => 'supplier_code',
        ];

        $rules = [
            'doc_no'           => 'nullable|string',
            'invoice_no'       => 'nullable|string',
            'invoice_date'     => 'nullable|date_format:Y-m-d',
            'po_no'            => 'nullable|string',
            'cn_ref_doc'       => 'nullable|string',
            'assignment'       => 'nullable|string',
            'amount'           => 'nullable|numeric',
            'vat'              => 'nullable|numeric',
            'net_amount'       => 'nullable|numeric',
            'start_round_date' => 'nullable|date_format:Y-m-d',
            'end_round_date'   => 'nullable|date_format:Y-m-d',
            'supplier_name'    => 'nullable|string',
            'supplier_code'    => 'nullable|string',
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

            // map คีย์
            $row = [];
            foreach ($thaiToDb as $th => $dbKey) {
                if (array_key_exists($th, $raw)) {
                    $row[$dbKey] = $raw[$th];
                }
            }

            if (!array_key_exists('cn_ref_doc', $row) || trim((string)($row['cn_ref_doc'] ?? '')) === '') {
                $row['cn_ref_doc'] = null;
            } elseif ($row['cn_ref_doc'] !== null) {
                $v = $row['cn_ref_doc'];
                if (is_int($v) || is_float($v)) {
                    $row['cn_ref_doc'] = preg_replace('/\.0+$/', '', (string)$v);
                } elseif (is_string($v)) {
                    $row['cn_ref_doc'] = preg_replace('/\.0+$/', '', trim($v));
                }
            }

            // แปลงตัวเลข
            foreach (['amount', 'vat', 'net_amount'] as $f) {
                $row[$f] = $this->toNumberOrNull($row[$f] ?? null);
            }

            // แปลงวันที่
            foreach (['invoice_date', 'start_round_date', 'end_round_date'] as $f) {
                $row[$f] = $this->toYmdOrNull($row[$f] ?? null);
            }

            // จำกัดเฉพาะคีย์ที่มีในตาราง
            $row = array_intersect_key($row, $columnsFlip);
            foreach ($row as $k => $v) {
                if (is_array($v) || is_object($v)) unset($row[$k]);
            }

            if ($hasTimestamps) {
                $row['created_at'] = $nowStr;
                $row['updated_at'] = $nowStr;
            }

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
                continue;
            }

            try {
                $dbTbl->insert($row);
                $inserted++;
            } catch (QueryException $e) {
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
        try {
            return Carbon::parse($s)->format('Y-m-d');
        } catch (\Throwable $e) {
            return null;
        }
    }

    protected function toNumberOrNull($v): ?float
    {
        if ($v === null) return null;
        if (is_float($v) || is_int($v)) return (float)$v;
        if (!is_string($v)) return null;
        $s = preg_replace('/[^\d\.\,\-]/u', '', trim($v));
        $s = str_replace(',', '', $s);
        if ($s === '' || $s === '.' || $s === '-') return null;
        if (!is_numeric($s)) return null;
        return (float)$s;
    }
}
