<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class CompanyPerson extends Model
{
    protected $table = 'company_person';

    protected $fillable = [
        'registered_no',
        'citizen_id',
        'prefix',
        'first_name',
        'last_name',
        'phone',
        'is_owner',
        'director_no',
        'boj5_doc_no',
    ];

    protected $casts = [
        'is_owner' => 'boolean',
        'director_no' => 'integer',
    ];
}
