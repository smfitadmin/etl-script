<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class FinancialDict extends Model
{
    protected $table = 'financial_dict';
    protected $primaryKey = 'item_code';
    public $incrementing = false;
    protected $keyType = 'string';

    protected $fillable = [
        'item_code',
        'name_th',
        'name_en',
        'category_code',
        'display_order',
        'is_total',
        'created_at',
        'updated_at',
    ];

    protected $casts = [
        'display_order' => 'integer',
        'is_total' => 'boolean',
    ];
}
