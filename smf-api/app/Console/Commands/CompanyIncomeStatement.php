<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class CompanyIncomeStatement extends Model
{
    use HasFactory;

    protected $table = 'company_income_statement';

    protected $fillable = [
        'registered_no',
        'fiscal_year',
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

    protected $casts = [
        'fiscal_year'            => 'integer',
        'net_revenue'            => 'decimal:2',
        'total_revenue'          => 'decimal:2',
        'cost_of_goods_sold'     => 'decimal:2',
        'gross_profit'           => 'decimal:2',
        'operating_expenses'     => 'decimal:2',
        'total_expenses'         => 'decimal:2',
        'interest_expenses'      => 'decimal:2',
        'profit_before_tax'      => 'decimal:2',
        'income_tax_expenses'    => 'decimal:2',
        'net_profit'             => 'decimal:2',
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
