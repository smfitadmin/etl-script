<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

class BolApiRaw extends Model
{
    use HasFactory;

    public $timestamps = false;

    protected $table = 'bol_api_raw';

    protected $fillable = [
        'registration_no',
        'raw_json',
    ];
}
