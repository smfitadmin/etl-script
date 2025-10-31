<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;

/**
 * Class CompanyEntity
 *
 * @property int $id
 * @property string|null $registered_no
 * @property string|null $company_name_en
 * @property string|null $company_name_th
 * @property string|null $entity_type_code
 * @property string|null $registration_date
 * @property string|null $company_status
 * @property string|null $company_size
 * @property int|null $num_director
 * @property float|null $registered_capital_baht
 * @property int|null $total_num_shares
 * @property float|null $value_per_share
 * @property bool|null $is_hq
 * @property string|null $branch
 * @property string|null $registered_address
 * @property string|null $address
 * @property int|null $contact_person_id
 * @property string|null $vat_registered_no
 * @property bool|null $is_vat
 * @property bool|null $is_ncb
 * @property bool|null $is_led
 * @property bool|null $is_secured
 * @property string|null $business_section_registration_code
 * @property string|null $business_section_latest_code
 * @property string|null $objective_at_registration
 * @property string|null $objective_latest
 * @property string|null $created_at
 * @property string|null $updated_at
 */
class CompanyEntity extends Model
{
    use HasFactory;

    protected $table = 'company_entity';

    protected $fillable = [
        'registered_no',
        'company_name_en',
        'company_name_th',
        'entity_type_code',
        'registration_date',
        'company_status',
        'company_size',
        'num_director',
        'registered_capital_baht',
        'total_num_shares',
        'value_per_share',
        'is_hq',
        'branch',
        'registered_address',
        'address',
        'contact_person_id',
        'vat_registered_no',
        'is_vat',
        'is_ncb',
        'is_led',
        'is_secured',
        'business_section_registration_code',
        'business_section_latest_code',
        'objective_at_registration',
        'objective_latest',
    ];

    protected $casts = [
        'registration_date' => 'date',
        'registered_capital_baht' => 'decimal:2',
        'value_per_share' => 'decimal:2',
        'is_hq' => 'boolean',
        'is_vat' => 'boolean',
        'is_ncb' => 'boolean',
        'is_led' => 'boolean',
        'is_secured' => 'boolean',
        'num_director' => 'integer',
        'total_num_shares' => 'integer',
        'contact_person_id' => 'integer',
    ];
}
