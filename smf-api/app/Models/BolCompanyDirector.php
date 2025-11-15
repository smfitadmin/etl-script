<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletes;

class BolCompanyDirector extends Model
{
    use SoftDeletes;

    protected $table = 'bol_company_directors';
    protected $primaryKey = 'id';
    public $incrementing = true;
    protected $keyType = 'int';

    // ✅ เปิด timestamps (Laravel จะจัดการ created_at และ updated_at ให้)
    public $timestamps = true;

    protected $fillable = [
        'registration_no',
        'director_no',
        'prefix_name',
        'first_name',
        'last_name'
    ];

    protected $casts = [
        'director_no' => 'integer',
        'created_at'  => 'datetime',
        'updated_at'  => 'datetime',
    ];
}
