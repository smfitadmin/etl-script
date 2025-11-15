<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class BolCompanyEntity extends Model
{
    protected $table = 'bol_company_entity';
    protected $primaryKey = 'id';
    public $incrementing = true;
    protected $keyType = 'int';

    public $timestamps = true;

    protected $fillable = [
        'registration_no',
        'company_name',
        'company_status',
        'address',
        'telephone_no',
        'registration_date',
        'registered_capital',
        'business_size',
        'company_type',
        'year_in_business',
        'registration_no_previous',
        'inactive_date',
        'importer_exporter',
        'sub_district',
        'district',
        'province',
        'region',
        'official_signatory',
        'tsic_code1',
        'description_tsic_code1',
        'tsic_code2',
        'description_tsic_code2',
        'tsic_code3',
        'description_tsic_code3',
        'naics_code1',
        'description_naics_code1',
        'naics_code2',
        'description_naics_code2',
        'naics_code3',
        'description_naics_code3',
        'fs_score',
        'fs_class_code',
        'description_fs_class_code',
        'company_credit_start',
        'company_credit_end',
        'credit_term_start',
        'credit_term_end',
    ];

    protected $casts = [
        'registration_date'          => 'date',
        'inactive_date'              => 'date',
        'registered_capital'         => 'decimal:2',
        'year_in_business'           => 'integer',
        'importer_exporter'          => 'integer',
        'fs_score'                   => 'integer',
        'company_credit_start'       => 'decimal:2',
        'company_credit_end'         => 'decimal:2',
        'credit_term_start'          => 'integer',
        'credit_term_end'            => 'integer',
        'created_at'                 => 'datetime',
        'updated_at'                 => 'datetime',
    ];
}
