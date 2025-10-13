<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class GecInvoice extends Model
{
    use HasFactory;

    protected $table = 'gec_invoices';

    public $timestamps = false;

    protected $fillable = [
        'invoice_no',
        'po_no',
        'invoice_date',
        'supplier_name',
        'buyer_name',
        'amount_excl_vat',
        'vat_amount',
        'amount_incl_vat'
    ];

    protected $casts = [
        'invoice_date' => 'date',
        'amount_excl_vat' => 'decimal:2',
        'vat_amount' => 'decimal:2',
        'amount_incl_vat' => 'decimal:2',
    ];
}
