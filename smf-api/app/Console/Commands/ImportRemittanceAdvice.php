<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class ImportRemittanceAdvice extends Command
{
    protected $signature = 'rem-advice:import
                            {file : JSON path เช่น storage/app/72195.json หรือ 72195.json}';

    protected $description = 'Import Remittance Advice rows เข้า rm_detail_report';

    public function handle(): int
    {
        $fileArg = $this->argument('file');

        // -------------------------------------------------------
        // Normalize path
        // -------------------------------------------------------
        if ($this->isAbsolutePath($fileArg)) {
            $jsonPath = $fileArg;
        } elseif (str_starts_with($fileArg, 'storage/app/')) {
            // convert from storage/app/xxx.json → storage_path('app/xxx.json')
            $relative = substr($fileArg, strlen('storage/app/'));
            $jsonPath = storage_path('app/' . $relative);
        } else {
            // default: storage/app/<fileArg>
            $jsonPath = storage_path('app/' . ltrim($fileArg, '/'));
        }

        if (!file_exists($jsonPath)) {
            $this->error("File not found: {$jsonPath}");
            return self::FAILURE;
        }

        $this->info("Reading: {$jsonPath}");

        $raw = file_get_contents($jsonPath);
        $data = json_decode($raw, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            $this->error("Invalid JSON: " . json_last_error_msg());
            return self::FAILURE;
        }

        // -------------------------------------------------------
        // Extract supplier_code from root if present
        // -------------------------------------------------------
        $rootSupplierCode = $data['supplier_code'] ?? null;

        if (!isset($data['sheets']) || !is_array($data['sheets'])) {
            $this->error('JSON invalid: missing "sheets"');
            return self::FAILURE;
        }

        $insertCount = 0;

        DB::beginTransaction();
        try {
            foreach ($data['sheets'] as $sheetIdx => $sheet) {
                if (!isset($sheet['rows']) || !is_array($sheet['rows'])) {
                    $this->warn("Sheet {$sheetIdx} ไม่มี rows ข้ามไป");
                    continue;
                }

                foreach ($sheet['rows'] as $rowIdx => $row) {
                    if (!is_array($row)) {
                        $this->warn("Row sheet={$sheetIdx}, row={$rowIdx} invalid");
                        continue;
                    }

                    $supplierCode = $row['supplier_code'] ?? $rootSupplierCode;

                    // ตรวจว่า supplier_code ต้องมี
                    if ($supplierCode === null) {
                        $this->warn("ข้าม row sheet={$sheetIdx}, row={$rowIdx} เพราะไม่มี supplier_code");
                        continue;
                    }

                    // INSERT
                    DB::table('rm_detail_report')->insert([
                        'report_date'   => null,
                        'payment_date'  => $row['วันที่จ่ายเงิน'] ?? null,
                        'invoice_date'  => $row['วันที่'] ?? null,
                        'supplier_code' => $supplierCode,
                        'branch_code'   => $row['รหัสสาขา'] ?? null,
                        'doc_type'      => $row['ประเภทเอกสาร'] ?? null,
                        'doc_no'        => $row['เลขที่เอกสาร'] ?? null,
                        'doc_ref_no'    => $row['เลขที่เอกสารอ้างอิง'] ?? null,
                        'rm_amount'     => $row['จำนวน'] ?? null,
                        // created_at / updated_at → let MySQL default
                    ]);

                    $insertCount++;
                }
            }

            DB::commit();
        } catch (\Throwable $e) {
            DB::rollBack();
            $this->error("Error: " . $e->getMessage());
            return self::FAILURE;
        }

        $this->info("Import สำเร็จ: {$insertCount} rows");

        return self::SUCCESS;
    }

    // -------------------------------------------------------
    // Check absolute path
    // -------------------------------------------------------
    private function isAbsolutePath(string $path): bool
    {
        // Windows เช่น C:\xxx
        if (preg_match('#^[A-Za-z]:[\\\\/]#', $path)) {
            return true;
        }

        // Linux/macOS absolute path เช่น /xxx
        return str_starts_with($path, '/');
    }
}
