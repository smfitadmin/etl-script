<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletes;

class BolCompanyShareholder extends Model
{
    use SoftDeletes;

    protected $table = 'bol_company_shareholders';
    protected $primaryKey = 'id';
    public $incrementing = true;
    protected $keyType = 'int';

    public $timestamps = true;

    protected $fillable = [
        'registration_no',
        'director_no',
        'prefix_name',
        'first_name',
        'last_name',
        'nationality',
        'percent_share',
        'no_of_share',
        'baht_share',
    ];

    protected $casts = [
        'registration_no' => 'string',
        'director_no'     => 'integer',
        'percent_share'   => 'decimal:2',
        'no_of_share'     => 'decimal:2',
        'baht_share'      => 'decimal:2',
        'created_at'      => 'datetime',
        'updated_at'      => 'datetime',
    ];
}
