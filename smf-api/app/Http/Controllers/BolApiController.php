<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

use App\Models\BolCompanyEntity;
use App\Models\BolCompanyDirector;
use App\Models\BolCompanyShareholder;
use App\Models\BolFinancial;
use App\Models\BolApiRaw;

class BolApiController extends Controller
{
    // POST /api/bol/store
    public function bol_store(Request $request)
    {
        if (empty(env('CPX_COLUMN_CODE'))) {
            return response()->json([
                'message' => 'no column code'
            ], 201);
        }

        if (empty($request['registration_no'])) {
            return response()->json([
                'message' => 'no registration no'
            ], 201);
        }

        try {
            // 1) LOGIN → ได้ [$token, $type]
            [$token, $type] = $this->login();

            // 2) GET DATA ด้วย payload จาก body
            $payload = [
                'systemId'       => '1',
                'registrationId' => $request['registration_no'],
                'status'         => '1',
                'dataSet'        => '',
                'dataField'      => env('CPX_COLUMN_CODE'),
                'periodFrom'     => '0',
                'periodTo'       => '0',
                'fsType'         => '2',
                'language'       => env('CPX_LANGUAGE', 'TH'),
            ];

            $resp = Http::asForm()
                ->withHeaders([
                    'Authorization' => $type . ' ' . $token,
                ])
                ->acceptJson()
                ->post(env('CPX_GETDATA_API'), $payload)
                ->throw()
                ->json();
            // $path = storage_path('app/example.json');
            // $resp = json_decode(file_get_contents($path), true);

            // บันทึกข้อมูลดิบจาก API
            BolApiRaw::create([
                'registration_no' => $request['registration_no'],
                'raw_json' => json_encode($resp, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT),
            ]);

            $results = $resp['searchResults'] ?? [];

            if (!is_array($results) || empty($results)) {
                return response()->json(['message' => 'no searchResults found'], 422);
            }

            // 3) STORE ลง DB
            $this->storeResults($results);

            // 4) LOGOUT (ไม่ throw หากล้มเหลว)
            $this->sessionClear();

            return response()->json([
                'message' => 'stored',
                'count'   => count($results),
            ], 200);
        } catch (\Throwable $e) {
            Log::error('bol_store failed', ['err' => $e->getMessage()]);
            return response()->json(['message' => 'error', 'error' => $e->getMessage()], 500);
        }
    }

    // ----- Helpers -----

    // login แบบ x-www-form-urlencoded ตาม Postman → คืน [token, type]
    private function login(): array
    {
        $resp = Http::asForm()
            ->acceptJson()
            ->post(env('CPX_LOGIN_API'), [
                'grant_type' => 'password',
                'username'   => env('CPX_USERNAME'),
                'password'   => env('CPX_PASSWORD'),
                'language'   => env('CPX_LANGUAGE', 'TH'),
            ])
            ->throw()
            ->json();
        // $path = storage_path('app/login_success.json');
        // $resp = json_decode(file_get_contents($path), true);

        $token = $resp['access_token'] ?? null;
        $type  = $resp['token_type']   ?? 'Bearer';

        if (!$token) {
            throw new \RuntimeException('Login failed: no access_token.');
        }
        return [$token, $type];
    }

    private function sessionClear(): array
    {
        $resp = Http::asForm()
            ->acceptJson()
            ->post(env('CPX_LOGOUT_API'), [
                'userName'   => env('CPX_USERNAME'),
                'password'   => env('CPX_PASSWORD'),
                'language'   => env('CPX_LANGUAGE', 'TH'),
            ])
            ->throw()
            ->json();
        return $resp;
    }

    private function storeResults(array $results): void
    {
        DB::transaction(function () use ($results) {
            foreach ($results as $r) {
                $regNo = $r['registrationNo'] ?? null;
                if (!$regNo) continue;

                $registrationDate = $this->toMysqlDateTime($r['registrationDate'] ?? null);
                $inactiveDate     = $this->toMysqlDateTime($r['inactiveDate'] ?? null);

                // ตัวเลขช่วง
                [$creditStart, $creditEnd] = $this->parseRange($r['companyCredit'] ?? null);
                [$termStart,   $termEnd] = $this->parseRange($r['creditTerm'] ?? null);

                // tsic/naics อาจเป็น array ว่าง -> null
                $tsic1 = $r['tsicCode1'] ?? [];
                $tsic2 = $r['tsicCode2'] ?? [];
                $tsic3 = $r['tsicCode3'] ?? [];
                $naic1 = $r['naicsCode1'] ?? [];
                $naic2 = $r['naicsCode2'] ?? [];
                $naic3 = $r['naicsCode3'] ?? [];

                // Company (upsert by registration_no)
                BolCompanyEntity::updateOrCreate(
                    ['registration_no' => (string)$regNo],
                    [
                        'company_name'   => $this->nullIfEmpty($r['companyName'] ?? null),
                        'company_status' => $this->nullIfEmpty($r['companyStatus'] ?? null),
                        'address'        => $this->nullIfEmpty($r['address'] ?? null),
                        'telephone_no'   => $this->nullIfEmpty($r['telephoneNo'] ?? null),

                        'registration_date'       => $registrationDate,               // <= null ได้
                        'registered_capital'      => $this->toFloatOrNull($r['registeredCapital'] ?? null),
                        'business_size'           => $this->nullIfEmpty($r['businessSize'] ?? null),
                        'company_type'            => $this->nullIfEmpty($r['companyType'] ?? null),
                        'year_in_business'        => $this->toFloatOrNull($r['yearInBusiness'] ?? null),
                        'registration_no_previous' => $this->nullIfEmpty($r['registrationNoPrevious'] ?? null),

                        'inactive_date'           => $inactiveDate,
                        'importer_exporter'       => $this->nullIfEmpty($r['importerExporter'] ?? null),
                        'sub_district'            => $this->nullIfEmpty($r['subDistrict'] ?? null),
                        'district'                => $this->nullIfEmpty($r['district'] ?? null),
                        'province'                => $this->nullIfEmpty($r['province'] ?? null),
                        'region'                  => $this->nullIfEmpty($r['region'] ?? null),
                        'official_signatory'      => $this->nullIfEmpty($r['officialSignatory'] ?? null),

                        'tsic_code1'              => $this->nullIfEmpty($tsic1['code'] ?? null),
                        'description_tsic_code1'  => $this->nullIfEmpty($tsic1['description'] ?? null),
                        'tsic_code2'              => $this->nullIfEmpty($tsic2['code'] ?? null),
                        'description_tsic_code2'  => $this->nullIfEmpty($tsic2['description'] ?? null),
                        'tsic_code3'              => $this->nullIfEmpty($tsic3['code'] ?? null),
                        'description_tsic_code3'  => $this->nullIfEmpty($tsic3['description'] ?? null),

                        'naics_code1'             => $this->nullIfEmpty($naic1['code'] ?? null),
                        'description_naics_code1' => $this->nullIfEmpty($naic1['description'] ?? null),
                        'naics_code2'             => $this->nullIfEmpty($naic2['code'] ?? null),
                        'description_naics_code2' => $this->nullIfEmpty($naic2['description'] ?? null),
                        'naics_code3'             => $this->nullIfEmpty($naic3['code'] ?? null),
                        'description_naics_code3' => $this->nullIfEmpty($naic3['description'] ?? null),

                        'fs_score'                => $this->toFloatOrNull($r['fsScore'] ?? null),
                        'fs_class_code'           => $this->nullIfEmpty(($r['fsClass']['code'] ?? null)),
                        'description_fs_class_code' => $this->nullIfEmpty(($r['fsClass']['description'] ?? null)),

                        'company_credit_start'    => $creditStart,
                        'company_credit_end'      => $creditEnd,
                        'credit_term_start'       => $termStart,
                        'credit_term_end'         => $termEnd,
                    ]
                );

                // Directors (replace-all)
                if (!empty($r['directors']) && is_array($r['directors'])) {
                    $incomingKeys = [];

                    foreach ($r['directors'] as $d) {
                        $full = $d['name'] ?? '';
                        [$prefix, $first, $last] = $this->splitThaiName($full);
                        $prefix = trim($prefix ?? '');
                        $first  = trim($first ?? '');
                        $last   = trim($last ?? '');
                        $incomingKeys[] = "{$prefix}|{$first}|{$last}";

                        BolCompanyDirector::withTrashed()
                            ->where('registration_no', (string)$regNo)
                            ->where('prefix_name', $prefix)
                            ->where('first_name', $first)
                            ->where('last_name', $last)
                            ->restore();

                        BolCompanyDirector::updateOrCreate(
                            [
                                'registration_no' => (string)$regNo,
                                'prefix_name'     => $prefix,
                                'first_name'      => $first,
                                'last_name'       => $last,
                            ],
                            [
                                'director_no'     => $d['no'] ?? null,
                            ]
                        );
                    }

                    $existing = BolCompanyDirector::where('registration_no', (string)$regNo)
                        ->get(['id', 'prefix_name', 'first_name', 'last_name']);

                    $toDelete = [];
                    foreach ($existing as $row) {
                        $key = "{$row->prefix_name}|{$row->first_name}|{$row->last_name}";
                        if (!in_array($key, $incomingKeys, true)) {
                            $toDelete[] = $row->id;
                        }
                    }

                    if (!empty($toDelete)) {
                        BolCompanyDirector::whereIn('id', $toDelete)->delete();
                    }
                } else {
                    BolCompanyDirector::where('registration_no', (string)$regNo)->delete();
                }


                // Shareholders (replace-all)
                if (!empty($r['shareholder']['shareholders']) && is_array($r['shareholder']['shareholders'])) {
                    $incomingKeys = [];

                    foreach ($r['shareholder']['shareholders'] as $s) {
                        $full = $s['name'] ?? '';
                        [$prefix, $first, $last] = $this->splitThaiName($full);
                        $prefix = trim($prefix ?? '');
                        $first  = trim($first ?? '');
                        $last   = trim($last ?? '');
                        $incomingKeys[] = "{$prefix}|{$first}|{$last}";

                        BolCompanyShareholder::withTrashed()
                            ->where('registration_no', (string)$regNo)
                            ->where('prefix_name', $prefix)
                            ->where('first_name',  $first)
                            ->where('last_name',   $last)
                            ->restore();

                        BolCompanyShareholder::updateOrCreate(
                            [
                                'registration_no' => (string)$regNo,
                                'prefix_name'     => $prefix,
                                'first_name'      => $first,
                                'last_name'       => $last,
                            ],
                            [
                                'director_no'     => $s['no'] ?? null,
                                'nationality'     => $s['nationality'] ?? null,
                                'percent_share'   => $this->toFloatOrNull($s['percentShare'] ?? null),
                                'no_of_share'     => $this->toFloatOrNull($s['noOfShare'] ?? null),
                                'baht_share'      => $this->toFloatOrNull($s['bahtShare'] ?? null),
                            ]
                        );
                    }

                    $existing = BolCompanyShareholder::where('registration_no', (string)$regNo)
                        ->get(['id', 'prefix_name', 'first_name', 'last_name']);

                    $toDelete = [];
                    foreach ($existing as $row) {
                        $key = "{$row->prefix_name}|{$row->first_name}|{$row->last_name}";
                        if (!in_array($key, $incomingKeys, true)) {
                            $toDelete[] = $row->id;
                        }
                    }
                    if ($toDelete) {
                        BolCompanyShareholder::whereIn('id', $toDelete)->delete(); // soft delete
                    }
                } else {
                    BolCompanyShareholder::where('registration_no', (string)$regNo)->delete();
                }


                // Financial (upsert by registered_no + fiscal_year)
                if (empty($r['financial']) || !is_array($r['financial'])) {
                    // ถ้าไม่มี block financial ก็ไม่ทำอะไร (ไม่ลบของเก่า)
                    return;
                }

                $seenYears = [];

                foreach ($r['financial'] as $f) {
                    // ----- ปีงบประมาณ -----
                    $fyRaw = $this->nullIfEmpty($f['fiscalYear'] ?? null);
                    if (!$fyRaw) {
                        continue;
                    }
                    $fy = (int) $fyRaw;
                    if ($fy > 2400) {
                        $fy -= 543;
                    }
                    if ($fy < 1000) {
                        continue;
                    }
                    $fyStr = (string) $fy;
                    $seenYears[] = $fyStr;

                    $financialDate = $this->toMysqlDate($f['financialDate'] ?? null);

                    BolFinancial::updateOrCreate(
                        [
                            'registration_no' => (string) $regNo,
                            'fiscal_year'     => $fyStr,
                        ],
                        [
                            'financial_date' => $financialDate,
                            'fs_type'        => $this->nullIfEmpty($f['fsType'] ?? null),

                            // สินทรัพย์/หนี้/ทุน
                            'total_assets'                          => $this->toFloatOrNull($f['totalAssets'] ?? null),
                            'retained_earning'                      => $this->toFloatOrNull($f['retainedEarning'] ?? null),
                            'total_revenue'                         => $this->toFloatOrNull($f['totalRevenue'] ?? null),
                            'gross_profit'                          => $this->toFloatOrNull($f['grossProfit'] ?? null),
                            'income_before_depreciation'            => $this->toFloatOrNull($f['incomeBeforeDepreciation'] ?? null),
                            'income_before_interest_and_income_taxes' => $this->toFloatOrNull($f['incomeBeforeInterestAndIncomeTaxes'] ?? null),
                            'net_income'                            => $this->toFloatOrNull($f['netIncome'] ?? null),

                            'account_receivable'                    => $this->toFloatOrNull($f['accountReceivable'] ?? null),
                            'account_notes_receivable_net'          => $this->toFloatOrNull($f['accountNotesReceivableNet'] ?? null),
                            'inventories'                           => $this->toFloatOrNull($f['inventories'] ?? null),
                            'short_term_loans_assets'               => $this->toFloatOrNull($f['shortTermLoansAssets'] ?? null),
                            'total_current_assets'                  => $this->toFloatOrNull($f['totalCurrentAssets'] ?? null),
                            'long_term_loans_assets'                => $this->toFloatOrNull($f['longTermLoansAssets'] ?? null),
                            'property_plant_equipment_net'          => $this->toFloatOrNull($f['propertyPlantEquipmentNet'] ?? null),
                            'total_non_current_assets'              => $this->toFloatOrNull($f['totalNonCurrentAssets'] ?? null),
                            'cash_and_deposits_at_financial_institutions' => $this->toFloatOrNull($f['cashAndDepositsAtFinancialInstitutions'] ?? null),

                            'accounts_payable'                      => $this->toFloatOrNull($f['accountsPayable'] ?? null),
                            'total_current_liabilities'             => $this->toFloatOrNull($f['totalCurrentLiabilities'] ?? null),
                            'total_non_current_liabilities'         => $this->toFloatOrNull($f['totalNonCurrentLiabilities'] ?? null),
                            'total_liabilities'                     => $this->toFloatOrNull($f['totalLiabilities'] ?? null),

                            'authorized_common_stocks'              => $this->toFloatOrNull($f['authorizedCommonStocks'] ?? null),
                            'issued_paid_up_common_stocks'          => $this->toFloatOrNull($f['issuedPaidUpCommonStocks'] ?? null),
                            'total_shareholders_equity'             => $this->toFloatOrNull($f['totalShareholdersEquity'] ?? null),
                            'total_liabilities_shareholders_equity' => $this->toFloatOrNull($f['totalLiabilitiesShareholdersEquity'] ?? null),

                            'net_sales'                             => $this->toFloatOrNull($f['netSales'] ?? null),
                            'cost_of_sales_services'                => $this->toFloatOrNull($f['costOfSalesServices'] ?? null),
                            'operating_expenses'                    => $this->toFloatOrNull($f['operatingExpenses'] ?? null),
                            'earnings_loss_per_share'               => $this->toFloatOrNull($f['earningsLossPerShare'] ?? null),
                            'short_term_loan'                       => $this->toFloatOrNull($f['shortTermLoan'] ?? null),
                            'interest_expenses'                     => $this->toFloatOrNull($f['interestExpenses'] ?? null),

                            // อัตราส่วน
                            'current_ratio'                         => $this->toFloatOrNull($f['currentRatio'] ?? null),
                            'quick_ratio'                           => $this->toFloatOrNull($f['quickRatio'] ?? null),
                            'accounts_receivable_turnover'          => $this->toFloatOrNull($f['accountsReceivableTurnover'] ?? null),
                            'accounts_payable_turnover'             => $this->toFloatOrNull($f['accountsPayableTurnover'] ?? null),
                            'average_payment_period'                => $this->toFloatOrNull($f['averagePaymentPeriod'] ?? null),
                            'inventory_turnover'                    => $this->toFloatOrNull($f['inventoryTurnover'] ?? null),
                            'collection_period'                     => $this->toFloatOrNull($f['collectionPeriod'] ?? null),
                            'day_sales_inventory'                   => $this->toFloatOrNull($f['daySalesInventory'] ?? null),

                            'gross_profit_margin_percent'           => $this->toFloatOrNull($f['grossProfitMarginPercent'] ?? null),
                            'net_profit_margin_percent'             => $this->toFloatOrNull($f['netProfitMarginPercent'] ?? null),
                            'roa'                                   => $this->toFloatOrNull($f['roa'] ?? null),
                            'roe'                                   => $this->toFloatOrNull($f['roe'] ?? null),
                            'debt_ratio'                            => $this->toFloatOrNull($f['debtRatio'] ?? null),
                            'debt_equity_ratio'                     => $this->toFloatOrNull($f['debtEquityRatio'] ?? null),
                            'interest_coverage'                     => $this->toFloatOrNull($f['interestCoverage'] ?? null),

                            // growth
                            'net_sales_growth_percent'              => $this->toFloatOrNull($f['netSalesGrowthPercent'] ?? null),
                            'total_revenue_growth_percent'          => $this->toFloatOrNull($f['totalRevenueGrowthPercent'] ?? null),
                            'net_profit_growth_percent'             => $this->toFloatOrNull($f['netProfitGrowthPercent'] ?? null),
                            'total_asset_growth_percent'            => $this->toFloatOrNull($f['totalAssetGrowthPercent'] ?? null),
                        ]
                    );
                }

                // ----- Replace-all: ลบปีที่ไม่อยู่ในรอบนี้ -----
                if (!empty($seenYears)) {
                    BolFinancial::where('registration_no', (string) $regNo)
                        ->whereNotIn('fiscal_year', array_values(array_unique($seenYears)))
                        ->delete(); // ถ้าเปิด SoftDeletes ใน BolFinancial เปลี่ยนเป็น soft delete ได้
                }
            }
        });
    }

    // ว่าง/ช่องว่าง/เครื่องหมายคำถาม -> null
    private function nullIfEmpty($v): ?string
    {
        $s = trim((string)($v ?? ''));
        $s = str_replace("\u{00A0}", '', $s); // ลบ non-breaking space
        return ($s === '' || $s === '?') ? null : $s;
    }

    private function toFloatOrNull($v): ?float
    {
        $s = $this->nullIfEmpty($v);
        if ($s === null) return null;
        $s = str_replace([',', ' '], '', $s); // ตัดคอมมาและช่องว่าง
        return is_numeric($s) ? (float)$s : null;
    }

    private function toIntOrNull($v): ?int
    {
        $s = $this->nullIfEmpty($v);
        if ($s === null) return null;
        $s = str_replace([',', ' '], '', $s);
        return is_numeric($s) ? (int)$s : null;
    }

    private function toMysqlDate(?string $d): ?string
    {
        $d = $this->nullIfEmpty($d);
        if ($d === null) return null;
        $parts = explode('/', $d);
        if (count($parts) !== 3) return null;
        [$dd, $mm, $yy] = $parts;
        $dd = (int)$dd;
        $mm = (int)$mm;
        $yy = (int)$yy;
        if ($yy > 2400) $yy -= 543; // พ.ศ. → ค.ศ.
        if ($yy < 1000 || $mm < 1 || $mm > 12 || $dd < 1 || $dd > 31) return null;
        return sprintf('%04d-%02d-%02d', $yy, $mm, $dd);
    }

    // แปลงวันที่ไทย "dd/mm/BBBB" (พ.ศ.) หรือ "dd/mm/yyyy" -> "YYYY-mm-dd 00:00:00"
    // ว่าง/ผิดรูปแบบ -> null
    private function toMysqlDateTime(?string $d): ?string
    {
        $d = $this->nullIfEmpty($d);
        if ($d === null) return null;
        $parts = explode('/', $d);
        if (count($parts) !== 3) return null;
        [$dd, $mm, $yy] = $parts;
        $dd = (int)$dd;
        $mm = (int)$mm;
        $yy = (int)$yy;
        if ($yy > 2400) $yy -= 543; // พ.ศ. -> ค.ศ.
        if ($yy < 1000 || $mm < 1 || $mm > 12 || $dd < 1 || $dd > 31) return null;
        return sprintf('%04d-%02d-%02d 00:00:00', $yy, $mm, $dd);
    }

    // "a - b" หรือ "a" -> [start,end] (float|null)
    private function parseRange(?string $v): array
    {
        $v = $this->nullIfEmpty($v);
        if ($v === null) return [null, null];
        if (strpos($v, '-') !== false) {
            [$a, $b] = array_map('trim', explode('-', $v, 2));
            return [$this->toFloatOrNull($a), $this->toFloatOrNull($b)];
        }
        return [$this->toFloatOrNull($v), null];
    }

    private function splitThaiName(string $full): array
    {
        // ลบ zero-width และ normalize white space
        $s = preg_replace('/[\x{200B}\x{200C}\x{200D}\x{FEFF}]/u', '', $full);
        $s = trim(preg_replace('/\p{Z}+/u', ' ', $s));
        if ($s === '') return ['', '', ''];

        // คำนำหน้า (escape จุดด้วย \.)
        $honorifics = [
            'นาย',
            'นางสาว',
            'นาง',
            'ดร\.',
            'ดร',
            'ผศ\.ดร\.',
            'ผศ\.',
            'รศ\.ดร\.',
            'รศ\.',
            'ศ\.ดร\.',
            'ศ\.',
            'คุณ',
            'Mr\.',
            'Ms\.',
            'Mrs\.'
        ];
        $pattern = '/^(' . implode('|', $honorifics) . ')\s*/u';

        $prefix = '';
        if (preg_match($pattern, $s, $m)) {
            $prefix = $m[1];
            $s = trim(preg_replace($pattern, '', $s, 1));
        }

        $parts = preg_split('/\s+/u', $s, 2);
        $first = trim($parts[0] ?? '');
        $last  = trim($parts[1] ?? '');

        return [$prefix, $first, $last];
    }
}
