<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class DbdSupplier extends Model
{
    use HasFactory;

    protected $table = 'dbd_suppliers';

    public $timestamps = false;

    protected $fillable = [
        'gec_no',
        'registration_id',
        'supplier_id',
        'is_supplier',
        'start_effective_date',
        'size',
        'supplier_name',
        'registration_date',
        'registered_capital',
        'trade_receivables_net',
        'inventory',
        'current_assets',
        'property_plant_equipment',
        'non_current_assets',
        'total_assets',
        'current_liabilities',
        'non_current_liabilities',
        'total_liabilities',
        'shareholders_equity',
        'liabilities_and_equity',
        'group_id',
        'main_revenue',
        'total_revenue_fs',
        'cost_of_goods_sold',
        'gross_profit',
        'selling_and_admin_expenses',
        'total_expenses',
        'interest_expense',
        'profit_before_tax',
        'income_tax',
        'net_profit',
        'no_of_buyer',
        'roa_percent',
        'roe_percent',
        'gross_profit_margin_percent',
        'operating_margin_percent',
        'net_margin_percent',
        'asset_turnover_ratio',
        'receivables_turnover_ratio',
        'inventory_turnover_ratio',
        'operating_expense_ratio',
        'current_ratio',
        'debt_to_asset_ratio',
        'asset_to_equity_ratio',
        'debt_to_equity_ratio'
    ];

    protected $casts = [
        'start_effective_date' => 'date',
        'registration_date' => 'date',

        'registered_capital' => 'decimal:2',
        'trade_receivables_net' => 'decimal:2',
        'inventory' => 'decimal:2',
        'current_assets' => 'decimal:2',
        'property_plant_equipment' => 'decimal:2',
        'non_current_assets' => 'decimal:2',
        'total_assets' => 'decimal:2',
        'current_liabilities' => 'decimal:2',
        'non_current_liabilities' => 'decimal:2',
        'total_liabilities' => 'decimal:2',
        'shareholders_equity' => 'decimal:2',
        'liabilities_and_equity' => 'decimal:2',

        'main_revenue' => 'decimal:2',
        'total_revenue_fs' => 'decimal:2',
        'cost_of_goods_sold' => 'decimal:2',
        'gross_profit' => 'decimal:2',
        'selling_and_admin_expenses' => 'decimal:2',
        'total_expenses' => 'decimal:2',
        'interest_expense' => 'decimal:2',
        'profit_before_tax' => 'decimal:2',
        'income_tax' => 'decimal:2',
        'net_profit' => 'decimal:2',

        'roa_percent' => 'decimal:2',
        'roe_percent' => 'decimal:2',
        'gross_profit_margin_percent' => 'decimal:2',
        'operating_margin_percent' => 'decimal:2',
        'net_margin_percent' => 'decimal:2',
        'asset_turnover_ratio' => 'decimal:2',
        'receivables_turnover_ratio' => 'decimal:2',
        'inventory_turnover_ratio' => 'decimal:2',
        'operating_expense_ratio' => 'decimal:2',
        'current_ratio' => 'decimal:2',
        'debt_to_asset_ratio' => 'decimal:2',
        'asset_to_equity_ratio' => 'decimal:2',
        'debt_to_equity_ratio' => 'decimal:2',

        'group_id' => 'integer',
        'no_of_buyer' => 'integer',
    ];
}
