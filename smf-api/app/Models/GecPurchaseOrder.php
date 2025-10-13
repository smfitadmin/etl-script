<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class GecPurchaseOrder extends Model
{
    use HasFactory;

    protected $table = 'gec_purchase_orders';

    public $timestamps = false;

    protected $fillable = [
        'po_no',
        'po_date',
        'supplier_name',
        'buyer_name',
        'delivery_date',
        'payment_term',
        'amount_excl_vat',
        'vat_amount',
        'amount_incl_vat'
    ];

    protected $casts = [
        'po_date' => 'date',
        'delivery_date' => 'date',
        'amount_excl_vat' => 'decimal:2',
        'vat_amount' => 'decimal:2',
        'amount_incl_vat' => 'decimal:2',
    ];
}
