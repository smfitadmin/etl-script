<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use App\Models\CompanyBalanceSheet;
use App\Models\CompanyIncomeStatement;
use App\Models\CompanyFinancialRatios;
use App\Models\CompanyEntity;

class ImportDbdFinancial extends Command
{
    protected $signature = 'dbd:import-financial
                            {--tax_id= : กรองเฉพาะเลขนิติบุคคล/ภาษี}
                            {--dry : ทดลองรัน ไม่เขียนฐานข้อมูล}';

    protected $description = 'Import DBD financial (balance, income, ratios) from storage/app/*_{balance|income|ratios}.json';

    private const BALANCE_COLS = [
        'accounts_receivable_net',
        'inventories',
        'current_assets',
        'property_plant_equipment',
        'non_current_assets',
        'total_assets',
        'current_liabilities',
        'non_current_liabilities',
        'total_liabilities',
        'shareholders_equity',
        'total_liabilities_and_shareholder_equity',
    ];

    private const INCOME_COLS = [
        'net_revenue',
        'total_revenue',
        'cost_of_goods_sold',
        'gross_profit',
        'operating_expenses',
        'total_expenses',
        'interest_expenses',
        'profit_before_tax',
        'income_tax_expenses',
        'net_profit',
    ];

    private const RATIOS_COLS = [
        'return_on_assets_percent',
        'return_on_equity_percent',
        'gross_profit_margin_percent',
        'operating_profit_margin_percent',
        'net_profit_margin_percent',
        'current_ratio_times',
        'accounts_receivable_turnover_times',
        'inventory_turnover_times',
        'accounts_payable_turnover_times',
        'total_asset_turnover_times',
        'operating_expense_ratio_percent',
        'total_assets_to_shareholders_equity_ratio_times',
        'total_liabilities_to_total_assets_ratio_times',
        'debt_to_equity_ratio_times',
        'debt_to_working_capital_ratio_times',
    ];

    /** นับยอดที่ข้ามเพราะไม่มี parent */
    private int $skippedNoParent = 0;

    public function handle(): int
    {
        $filterTaxRaw = trim((string) $this->option('tax_id'));
        $filterTax    = $filterTaxRaw !== '' ? $this->normTaxId($filterTaxRaw) : '';
        $isDry        = (bool) $this->option('dry');

        $base = storage_path('app');

        $patterns = [
            $base . DIRECTORY_SEPARATOR . '*_balance.json',
            $base . DIRECTORY_SEPARATOR . '*_income.json',
            $base . DIRECTORY_SEPARATOR . '*_ratios.json',
        ];

        $files = [];
        foreach ($patterns as $p) {
            $files = array_merge($files, glob($p) ?: []);
        }
        sort($files);

        if (!$files) {
            $this->warn('ไม่พบไฟล์ *_balance.json / *_income.json / *_ratios.json ใน storage/app');
            return self::SUCCESS;
        }

        $this->info('== Import DBD Financial (Balance / Income / Ratios) ==');
        $this->line('- Base folder: ' . $base);
        $this->line('- Filter tax : ' . ($filterTax ?: '(none)'));
        $this->line('- Dry-run     : ' . ($isDry ? 'YES' : 'NO'));
        $this->newLine();

        $totalFiles   = 0;
        $totalYears   = 0;
        $totalColumns = 0;

        foreach ($files as $path) {
            $file = basename($path); // ex: 0105537086874_balance.json
            [$taxIdRawFromFile, $type] = $this->parseFileName($file);
            if (!$taxIdRawFromFile || !$type) {
                $this->warn("ข้ามไฟล์ (ชื่อไม่ตรงรูปแบบ): {$file}");
                continue;
            }

            $taxId = $this->normTaxId($taxIdRawFromFile);
            if ($filterTax && $filterTax !== $taxId) continue;

            $json = File::get($path);
            $data = json_decode($json, true);
            if (!is_array($data)) {
                $this->warn("ข้ามไฟล์ (JSON ไม่ถูกต้อง): {$file}");
                continue;
            }

            $this->line("ไฟล์: {$file}  [tax={$taxId}, type={$type}]");

            // ✅ ไม่แตะต้อง company_entity: ถ้าไม่มี parent -> ข้ามทั้งไฟล์นี้
            if (!$this->hasParent($taxId)) {
                $this->warn("  ⚠ ข้าม: ไม่มี parent ใน company_entity.registered_no={$taxId}");
                $this->skippedNoParent++;
                continue;
            }

            [$years, $cols] = $this->importByType($type, $taxId, $data, $isDry);

            $this->info("  ✔ ปีที่อัปเดต: {$years}, คอลัมน์ที่ตั้งค่า: {$cols}");
            $totalFiles++;
            $totalYears   += $years;
            $totalColumns += $cols;
        }

        $this->newLine();
        $this->info("สรุปทั้งหมด: ไฟล์={$totalFiles}, ปีรวม={$totalYears}, คอลัมน์รวม={$totalColumns}");
        if ($this->skippedNoParent > 0) {
            $this->warn("ข้ามเพราะไม่มี parent: {$this->skippedNoParent} ไฟล์");
        }
        return self::SUCCESS;
    }

    /** -------- Utilities -------- */

    private function normTaxId($v): string
    {
        $s = preg_replace('/\D+/', '', (string) $v);
        if (strlen($s) > 13) $s = substr($s, -13);
        return str_pad($s, 13, '0', STR_PAD_LEFT);
    }

    /** ตรวจว่ามี parent ใน company_entity หรือไม่ (ไม่สร้าง) */
    private function hasParent(string $taxId): bool
    {
        // ใช้ Eloquent/QueryBuilder ก็ได้
        return CompanyEntity::where('registered_no', $taxId)->exists();
    }

    private function parseFileName(string $file): array
    {
        if (!str_ends_with($file, '.json')) return [null, null];
        $main = substr($file, 0, -5);
        $pos  = strrpos($main, '_');
        if ($pos === false) return [null, null];
        $tax  = substr($main, 0, $pos);
        $type = substr($main, $pos + 1);
        $type = in_array($type, ['balance', 'income', 'ratios'], true) ? $type : null;
        return [$tax ?: null, $type];
    }

    private function importByType(string $type, string $taxId, array $byYearData, bool $isDry): array
    {
        return match ($type) {
            'balance' => $this->importBalance($taxId, $byYearData, $isDry),
            'income'  => $this->importIncome($taxId, $byYearData, $isDry),
            'ratios'  => $this->importRatios($taxId, $byYearData, $isDry),
            default   => [0, 0],
        };
    }

    private function importBalance(string $taxId, array $byYear, bool $isDry): array
    {
        $years = 0;
        $colsTotal = 0;
        foreach ($byYear as $year => $rows) {
            if (!is_numeric($year) || !is_array($rows)) continue;
            $year = (int) $year;

            $payload = ['tax_id' => $taxId, 'fiscal_year' => $year];
            $cols = 0;

            foreach ($rows as $row) {
                if (!is_array($row)) continue;
                $code   = $row['item_en'] ?? null;
                $amount = $row['amount']  ?? null;
                if ($code && in_array($code, self::BALANCE_COLS, true)) {
                    $payload[$code] = is_null($amount) ? null : (float) $amount;
                    $cols++;
                }
            }

            if ($isDry) {
                $this->line("    DRY: BS upsert {$taxId} {$year} set {$cols} cols");
            } else {
                CompanyBalanceSheet::updateOrCreate(
                    ['registered_no' => $taxId, 'fiscal_year' => $year],
                    $payload
                );
            }
            $years++;
            $colsTotal += $cols;
        }
        return [$years, $colsTotal];
    }

    private function importIncome(string $taxId, array $byYear, bool $isDry): array
    {
        $years = 0;
        $colsTotal = 0;
        foreach ($byYear as $year => $rows) {
            if (!is_numeric($year) || !is_array($rows)) continue;
            $year = (int) $year;

            $payload = ['tax_id' => $taxId, 'fiscal_year' => $year];
            $cols = 0;

            foreach ($rows as $row) {
                if (!is_array($row)) continue;
                $code   = $row['item_en'] ?? null;
                $amount = $row['amount']  ?? null;
                if ($code && in_array($code, self::INCOME_COLS, true)) {
                    $payload[$code] = is_null($amount) ? null : (float) $amount;
                    $cols++;
                }
            }

            if ($isDry) {
                $this->line("    DRY: IS upsert {$taxId} {$year} set {$cols} cols");
            } else {
                CompanyIncomeStatement::updateOrCreate(
                    ['registered_no' => $taxId, 'fiscal_year' => $year],
                    $payload
                );
            }
            $years++;
            $colsTotal += $cols;
        }
        return [$years, $colsTotal];
    }

    private function importRatios(string $taxId, array $byYear, bool $isDry): array
    {
        $years = 0;
        $colsTotal = 0;
        foreach ($byYear as $year => $rows) {
            if (!is_numeric($year) || !is_array($rows)) continue;
            $year = (int) $year;

            $payload = ['tax_id' => $taxId, 'fiscal_year' => $year];
            $cols = 0;

            foreach ($rows as $row) {
                if (!is_array($row)) continue;
                $code   = $row['item_en'] ?? null;
                $amount = $row['amount']  ?? null;
                if ($code && in_array($code, self::RATIOS_COLS, true)) {
                    $payload[$code] = is_null($amount) ? null : (float) $amount;
                    $cols++;
                }
            }

            if ($isDry) {
                $this->line("    DRY: RT upsert {$taxId} {$year} set {$cols} cols");
            } else {
                CompanyFinancialRatios::updateOrCreate(
                    ['registered_no' => $taxId, 'fiscal_year' => $year],
                    $payload
                );
            }
            $years++;
            $colsTotal += $cols;
        }
        return [$years, $colsTotal];
    }
}
