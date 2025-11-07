<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class CompanyFinancialRatios extends Model
{
    use HasFactory;

    protected $table = 'company_financial_ratios';

    protected $fillable = [
        'registered_no',
        'fiscal_year',
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

    protected $casts = [
        'fiscal_year'                                   => 'integer',
        'return_on_assets_percent'                      => 'decimal:2',
        'return_on_equity_percent'                      => 'decimal:2',
        'gross_profit_margin_percent'                   => 'decimal:2',
        'operating_profit_margin_percent'               => 'decimal:2',
        'net_profit_margin_percent'                     => 'decimal:2',

        'current_ratio_times'                           => 'decimal:2',
        'accounts_receivable_turnover_times'            => 'decimal:2',
        'inventory_turnover_times'                      => 'decimal:2',
        'accounts_payable_turnover_times'               => 'decimal:2',
        'total_asset_turnover_times'                    => 'decimal:2',
        'operating_expense_ratio_percent'               => 'decimal:2',
        'total_assets_to_shareholders_equity_ratio_times' => 'decimal:2',
        'total_liabilities_to_total_assets_ratio_times' => 'decimal:2',
        'debt_to_equity_ratio_times'                    => 'decimal:2',
        'debt_to_working_capital_ratio_times'           => 'decimal:2',
    ];

    // ---------- Query Scopes ----------
    public function scopeTax($q, string $taxId)
    {
        return $q->where('registered_no', $taxId);
    }

    public function scopeYear($q, int $year)
    {
        return $q->where('fiscal_year', $year);
    }
}
