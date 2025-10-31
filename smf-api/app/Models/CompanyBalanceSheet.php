<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class CompanyBalanceSheet extends Model
{
    protected $table = 'company_balance_sheet';

    protected $fillable = [
        'tax_id',
        'fiscal_year',
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
        'created_at',
        'updated_at',
    ];

    protected $casts = [
        'fiscal_year' => 'integer',
        'accounts_receivable_net' => 'decimal:2',
        'inventories' => 'decimal:2',
        'current_assets' => 'decimal:2',
        'property_plant_equipment' => 'decimal:2',
        'non_current_assets' => 'decimal:2',
        'total_assets' => 'decimal:2',
        'current_liabilities' => 'decimal:2',
        'non_current_liabilities' => 'decimal:2',
        'total_liabilities' => 'decimal:2',
        'shareholders_equity' => 'decimal:2',
        'total_liabilities_and_shareholder_equity' => 'decimal:2',
    ];
}
