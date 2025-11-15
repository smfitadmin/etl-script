<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Storage;

class ImportRemReport extends Command
{
    protected $signature = 'rem-report:import {jsonPath}';
    protected $description = 'Import remittance report JSON into rm_report table';

    public function handle()
    {
        $inputPath = $this->argument('jsonPath');

        /**
         * ------------------------------
         * รองรับทั้ง 2 แบบ:
         * 1) remittance_detail_72195.json
         * 2) storage/app/remittance_detail_72195.json
         * ------------------------------
         */

        // ตัด prefix "storage/app/" ออกถ้ามี
        $normalized = preg_replace('/^storage\/app\//', '', $inputPath);

        // ตรวจสอบไฟล์ (แบบ Laravel storage)
        if (Storage::exists($normalized)) {
            $fullPath = storage_path('app/' . $normalized);
        } else {
            // ถ้าไม่เจอ ให้ลองมองว่าเป็น full path จริง
            $fullPath = $inputPath;
        }

        if (!file_exists($fullPath)) {
            $this->error("File not found: " . $inputPath);
            return 1;
        }

        $json = file_get_contents($fullPath);
        $data = json_decode($json, true);

        if (!is_array($data)) {
            $this->error("Invalid JSON format");
            return 1;
        }

        $this->info("Importing " . count($data) . " rows...");

        foreach ($data as $row) {

            $insert = [
                'remittance_no'   => $row['remittance_no'] ?? null,
                'supplier_code'   => $row['supplier_code'] ?? null,
                'supplier_name'   => $row['supplier_name'] ?? null,
                'branch'          => $row['branch'] ?? null,
                'remittance_date' => $row['remittance_date'] ?? null,
                'pay_date'        => $row['pay_date'] ?? null,
                'sent_date'       => $row['sent_date'] ?? null,
                'amount'          => $row['amount'] ?? 0,
                'status'          => $row['status'] ?? null,
                'created_at'      => now(),
            ];

            DB::table('rm_report')->insert($insert);
        }

        $this->info("Import completed!");
        return 0;
    }
}
