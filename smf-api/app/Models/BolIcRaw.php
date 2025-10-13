<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class BolIcRaw extends Model
{
    use HasFactory;

    protected $table = 'bol_ic_raw'; // ชื่อตารางในฐานข้อมูล

    public $timestamps = false; // ถ้าตารางไม่มีคอลัมน์ created_at และ updated_at

    protected $fillable = [
        'company_id',
        'company_name',
        'year',
        'net_sales',
        'total_other_income',
        'total_revenue',
        'cost_of_sales_services',
        'gross_profit_loss',
        'total_operating_expenses',
        'operating_income_loss',
        'other_expenses',
        'income_loss_before_depreciation_and_amoritization',
        'income_loss_before_interest_and_income_taxes',
        'interest_expenses',
        'income_taxes',
        'extraordinary_items',
        'others',
        'net_income_loss',
        'earnings_loss_per_share',
        'number_of_weighted_average_ordinary_shares',
    ];

    protected $casts = [
        'year' => 'integer',
        'net_sales' => 'decimal:2',
        'total_other_income' => 'decimal:2',
        'total_revenue' => 'decimal:2',
        'cost_of_sales_services' => 'decimal:2',
        'gross_profit_loss' => 'decimal:2',
        'total_operating_expenses' => 'decimal:2',
        'operating_income_loss' => 'decimal:2',
        'other_expenses' => 'decimal:2',
        'income_loss_before_depreciation_and_amoritization' => 'decimal:2',
        'income_loss_before_interest_and_income_taxes' => 'decimal:2',
        'interest_expenses' => 'decimal:2',
        'income_taxes' => 'decimal:2',
        'extraordinary_items' => 'decimal:2',
        'others' => 'decimal:2',
        'net_income_loss' => 'decimal:2',
        'earnings_loss_per_share' => 'decimal:2',
        'number_of_weighted_average_ordinary_shares' => 'decimal:2',
    ];
}
