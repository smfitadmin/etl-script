<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class GecInvoice2023 extends Model
{
    use HasFactory;

    protected $table = 'gec_2023_invoices';

    public $timestamps = false;

    protected $fillable = [
        'invoice_no',
        'invoice_date',
        'po_no',
        'po_date',
        'supplier_code',
        'buyer_code',
        'amount_excl_vat',
        'vat_amount',
        'amount_incl_vat'
    ];

    protected $casts = [
        'amount_excl_vat' => 'decimal:2',
        'vat_amount' => 'decimal:2',
        'amount_incl_vat' => 'decimal:2',
    ];
}
