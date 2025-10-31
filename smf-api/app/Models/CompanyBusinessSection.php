<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

/**
 * Class CompanyBusinessSection
 *
 * @property int $id
 * @property string|null $code
 * @property string|null $description
 */
class CompanyBusinessSection extends Model
{
    use HasFactory;

    protected $table = 'company_business_section';

    public $timestamps = false;

    protected $fillable = [
        'code',
        'description',
    ];

    protected $casts = [
        'code' => 'string',
        'description' => 'string',
    ];

    /**
     * Example relationship (optional):
     * หากในอนาคตต้องการให้ company_entity มีความสัมพันธ์กับหมวดธุรกิจ
     * เช่น company_entity.business_section_registration_code → company_business_section.code
     * สามารถเพิ่ม belongsTo / hasMany ได้
     */
    public function businessSectionRegistration()
    {
        return $this->belongsTo(CompanyBusinessSection::class, 'business_section_registration_code', 'code');
    }

    public function businessSectionLatest()
    {
        return $this->belongsTo(CompanyBusinessSection::class, 'business_section_latest_code', 'code');
    }
}
