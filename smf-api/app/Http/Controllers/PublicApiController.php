<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use App\Models\BolBsRaw;
use App\Models\BolIcRaw;
use App\Models\GecInvoice;
use App\Models\GecPurchaseOrder;
use App\Models\DbdSupplier;
use App\Models\GecInvoice2023;
use App\Models\CompanyEntity;
use App\Models\CompanyPerson;
use App\Models\CompanyBusinessSection;
use App\Models\CompanyBalanceSheet;
use App\Models\CompanyIncomeStatement;
use App\Models\CompanyFinancialRatios;

use Illuminate\Validation\ValidationException;
use Illuminate\Support\Facades\DB;

class PublicApiController extends Controller
{
    public function bol_bs_store(Request $request)
    {
        // Mapping: Request key -> Database field
        $fieldMap = [
            'company_id' => 'company_id',
            'company_name' => 'company_name',
            'year' => 'year',
            'Assets' => 'assets',
            'Cash and deposits at financial institutions' => 'cash_and_deposits_at_financial_institutions',
            'Accounts receivable' => 'accounts_receivable',
            'Accounts and notes receivable - net' => 'accounts_and_notes_receivable_net',
            'Total short-term loans consolidation' => 'total_short_term_loans_consolidation',
            'Inventories-net' => 'inventories_net',
            'Accrued income' => 'accrued_income',
            'Prepaid expenses' => 'prepaid_expenses',
            'Other current assets' => 'other_current_assets',
            'Others - Total current assets' => 'others_total_current_assets',
            'Total current assets' => 'total_current_assets',
            'Total long-term loans and investments' => 'total_long_term_loans_and_investments',
            'Property, plant and equipment - net' => 'property_plant_and_equipment_net',
            'Other non-current assets' => 'other_non_current_assets',
            'Others - Total non-current assets' => 'others_total_non_current_assets',
            'Total non-current assets' => 'total_non_current_assets',
            'Total assets' => 'total_assets',
            "Liabilities and shareholders' equity" => 'liabilities_and_shareholders_equity',
            'Liabilities' => 'liabilities',
            'Bank overdrafts and short-term loans from financial institutions' => 'bank_overdrafts_and_short_term_loans_from_financial_institutions',
            'Accounts payable' => 'accounts_payable',
            'Total accounts payable and notes payable' => 'total_accounts_payable_and_notes_payable',
            'Current portion of long-term loans' => 'current_portion_of_long_term_loans',
            'Total short-term loans' => 'total_short_term_loans',
            'Accrued expenses' => 'accrued_expenses',
            'Unearned revenues' => 'unearned_revenues',
            'Other current liabilities' => 'other_current_liabilities',
            'Others - Total current liabilities' => 'others_total_current_liabilities',
            'Total current liabilities' => 'total_current_liabilities',
            'Total long-term loans' => 'total_long_term_loans',
            'Other non-current liabilities' => 'other_non_current_liabilities',
            'Others - Total non-current liabilities' => 'others_total_non_current_liabilities',
            'Total non-current liabilities' => 'total_non_current_liabilities',
            'Total Liabilities' => 'total_liabilities',
            "Shareholder's equity" => 'shareholders_equity',
            'Authorized preferred stocks' => 'authorized_preferred_stocks',
            'Authorized common stocks' => 'authorized_common_stocks',
            'Issued and paid-up preferred stocks' => 'issued_and_paid_up_preferred_stocks',
            'Issued and paid-up common stocks' => 'issued_and_paid_up_common_stocks',
            'Appraisal surplus on property, plant and equipment' => 'appraisal_surplus_on_property_plant_and_equipment',
            'Accumulated retained earnings' => 'accumulated_retained_earnings',
            'Others' => 'others',
            'Total shareholders\' equity' => 'total_shareholders_equity',
            'Total liabilities and shareholders\' equity' => 'total_liabilities_and_shareholders_equity',
            'Additional information for shareholders\' equity' => 'additional_information_for_shareholders_equity',
            'Common stocks' => 'common_stocks',
            'No.of shares - Authorized' => 'no_of_shares_authorized',
            'Par value (Baht) - Authorized' => 'par_value_baht_authorized',
            'No.of shares - Issued and paid-up' => 'no_of_shares_issued_and_paid_up',
            'Par value (Baht) - Issued and Paid-up' => 'par_value_baht_issued_and_paid_up',
        ];

        $data = [];
        foreach ($request->all() as $key => $value) {
            $mappedItem = [];
            foreach ($value as $k => $v) {
                if (isset($fieldMap[$k])) {
                    $mappedItem[$fieldMap[$k]] = $v;
                }
            }
            $data[] = $mappedItem;
        }

        $checked = [];
        foreach ($data as $kk => $item) {
            $validated = validator($item, [
                'company_id' => 'required|string',
                'company_name' => 'nullable|string',
                'year' => 'required|integer',
                'assets' => 'nullable|numeric',
                'cash_and_deposits_at_financial_institutions' => 'nullable|numeric',
                'accounts_receivable' => 'nullable|numeric',
                'accounts_and_notes_receivable_net' => 'nullable|numeric',
                'total_short_term_loans_consolidation' => 'nullable|numeric',
                'inventories_net' => 'nullable|numeric',
                'accrued_income' => 'nullable|numeric',
                'prepaid_expenses' => 'nullable|numeric',
                'other_current_assets' => 'nullable|numeric',
                'others_total_current_assets' => 'nullable|numeric',
                'total_current_assets' => 'nullable|numeric',
                'total_long_term_loans_and_investments' => 'nullable|numeric',
                'property_plant_and_equipment_net' => 'nullable|numeric',
                'other_non_current_assets' => 'nullable|numeric',
                'others_total_non_current_assets' => 'nullable|numeric',
                'total_non_current_assets' => 'nullable|numeric',
                'total_assets' => 'nullable|numeric',
                'liabilities_and_shareholders_equity' => 'nullable|numeric',
                'liabilities' => 'nullable|numeric',
                'bank_overdrafts_and_short_term_loans_from_financial_institutions' => 'nullable|numeric',
                'accounts_payable' => 'nullable|numeric',
                'total_accounts_payable_and_notes_payable' => 'nullable|numeric',
                'current_portion_of_long_term_loans' => 'nullable|numeric',
                'total_short_term_loans' => 'nullable|numeric',
                'accrued_expenses' => 'nullable|numeric',
                'unearned_revenues' => 'nullable|numeric',
                'other_current_liabilities' => 'nullable|numeric',
                'others_total_current_liabilities' => 'nullable|numeric',
                'total_current_liabilities' => 'nullable|numeric',
                'total_long_term_loans' => 'nullable|numeric',
                'other_non_current_liabilities' => 'nullable|numeric',
                'others_total_non_current_liabilities' => 'nullable|numeric',
                'total_non_current_liabilities' => 'nullable|numeric',
                'total_liabilities' => 'nullable|numeric',
                'shareholders_equity' => 'nullable|numeric',
                'authorized_preferred_stocks' => 'nullable|numeric',
                'authorized_common_stocks' => 'nullable|numeric',
                'issued_and_paid_up_preferred_stocks' => 'nullable|numeric',
                'issued_and_paid_up_common_stocks' => 'nullable|numeric',
                'appraisal_surplus_on_property_plant_and_equipment' => 'nullable|numeric',
                'accumulated_retained_earnings' => 'nullable|numeric',
                'others' => 'nullable|numeric',
                'total_shareholders_equity' => 'nullable|numeric',
                'total_liabilities_and_shareholders_equity' => 'nullable|numeric',
                'additional_information_for_shareholders_equity' => 'nullable|numeric',
                'common_stocks' => 'nullable|numeric',
                'no_of_shares_authorized' => 'nullable|integer',
                'par_value_baht_authorized' => 'nullable|numeric',
                'no_of_shares_issued_and_paid_up' => 'nullable|integer',
                'par_value_baht_issued_and_paid_up' => 'nullable|numeric',
            ])->validate();

            $checked[] = $validated;
        }

        $output = [];

        foreach ($checked as $item) {
            $bolBs = BolBsRaw::updateOrCreate(
                [
                    'company_id' => $item['company_id'],
                    'year' => $item['year']
                ],
                $item
            );

            $output[] = $bolBs;
        }

        return response()->json([
            'success' => true,
            'data' => $output,
            'message' => 'All data saved successfully.',
        ]);
    }

    public function bol_ic_store(Request $request)
    {
        // Mapping: Request key -> Database field
        $fieldMap = [
            'company_id' => 'company_id',
            'company_name' => 'company_name',
            'year' => 'year',
            'Net Sales' => 'net_sales',
            'Total other income' => 'total_other_income',
            'Total revenue' => 'total_revenue',
            'Cost of sales /services' => 'cost_of_sales_services',
            'Gross profit (loss)' => 'gross_profit_loss',
            'Total operating expenses' => 'total_operating_expenses',
            'Operating income (loss)' => 'operating_income_loss',
            'Other expenses' => 'other_expenses',
            'Income (loss) before depreciation and amortization' => 'income_loss_before_depreciation_and_amoritization',
            'Income (loss) before interest and income taxes' => 'income_loss_before_interest_and_income_taxes',
            'Interest expenses' => 'interest_expenses',
            'Income taxes' => 'income_taxes',
            'Extraordinary items' => 'extraordinary_items',
            'Others' => 'others',
            'Net income (loss)' => 'net_income_loss',
            'Earnings (loss) per share' => 'earnings_loss_per_share',
            'Number of Weighted Average Ordinary Shares' => 'number_of_weighted_average_ordinary_shares'
        ];

        $data = [];
        foreach ($request->all() as $key => $value) {
            $mappedItem = [];
            foreach ($value as $k => $v) {
                if (isset($fieldMap[$k])) {
                    $mappedItem[$fieldMap[$k]] = $v;
                }
            }
            $data[] = $mappedItem;
        }

        $checked = [];
        foreach ($data as $kk => $item) {
            $validated = validator($item, [
                'company_id' => 'required|string',
                'company_name' => 'nullable|string',
                'year' => 'required|integer',
                'net_sales' => 'nullable|numeric',
                'total_other_income' => 'nullable|numeric',
                'total_revenue' => 'nullable|numeric',
                'cost_of_sales_services' => 'nullable|numeric',
                'gross_profit_loss' => 'nullable|numeric',
                'total_operating_expenses' => 'nullable|numeric',
                'operating_income_loss' => 'nullable|numeric',
                'other_expenses' => 'nullable|numeric',
                'income_loss_before_depreciation_and_amoritization' => 'nullable|numeric',
                'income_loss_before_interest_and_income_taxes' => 'nullable|numeric',
                'interest_expenses' => 'nullable|numeric',
                'income_taxes' => 'nullable|numeric',
                'extraordinary_items' => 'nullable|numeric',
                'others' => 'nullable|numeric',
                'net_income_loss' => 'nullable|numeric',
                'earnings_loss_per_share' => 'nullable|numeric',
                'number_of_weighted_average_ordinary_shares' => 'nullable|numeric',
            ])->validate();

            $checked[] = $validated;
        }

        $output = [];

        foreach ($checked as $item) {
            $bolIc = BolIcRaw::updateOrCreate(
                [
                    'company_id' => $item['company_id'],
                    'year' => $item['year']
                ],
                $item
            );

            $output[] = $bolIc;
        }

        return response()->json([
            'success' => true,
            'data' => $output,
            'message' => 'All data saved successfully.',
        ]);
    }

    public function gec_po_store(Request $request)
    {
        try {

            $checked = [];
            foreach ($request->all() as $item) {
                $validated = validator($item, [
                    'po_no' => 'nullable|string',
                    'po_date' => 'nullable|date',
                    'supplier_name' => 'nullable|string',
                    'buyer_name' => 'nullable|string',
                    'delivery_date' => 'nullable|date',
                    'payment_term' => 'nullable|string',
                    'amount_excl_vat' => 'nullable|numeric',
                    'vat_amount' => 'nullable|numeric',
                    'amount_incl_vat' => 'nullable|numeric'
                ])->validate();
                $checked[] = $validated;
            }
            $output = [];

            foreach ($checked as $item) {
                $gecPo = GecPurchaseOrder::create(
                    // [
                    //     'po_no' => $item['po_no']
                    // ],
                    $item
                );

                $output[] = $gecPo;
            }

            return response()->json([
                'success' => true,
                'count_req' => count($request->all()),
                'count_output' => count($output),
                'data' => $output,
            ]);
        } catch (ValidationException $e) {
            return response()->json([
                'success' => false,
                'error' => 'Validation failed',
                'messages' => $e->errors(),
            ], 422);
        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    public function gec_invoice_store(Request $request)
    {
        try {
            $checked = [];

            foreach ($request->all() as $item) {
                $validated = validator($item, [
                    'invoice_no' => 'nullable|string',
                    'po_no' => 'nullable|string',
                    'invoice_date' => 'nullable|date',
                    'supplier_name' => 'nullable|string',
                    'buyer_name' => 'nullable|string',
                    'amount_excl_vat' => 'nullable|numeric',
                    'vat_amount' => 'nullable|numeric',
                    'amount_incl_vat' => 'nullable|numeric'
                ])->validate();

                $checked[] = $validated;
            }

            $output = [];

            foreach ($checked as $item) {
                $gecInv = GecInvoice::create(
                    // [
                    //     'invoice_no' => $item['invoice_no'],
                    //     'po_no' => $item['po_no']
                    // ],
                    $item
                );

                $output[] = $gecInv;
            }

            return response()->json([
                'success' => true,
                'count_req' => count($request->all()),
                'count_output' => count($output),
                'data' => $output,
            ]);
        } catch (ValidationException $e) {
            return response()->json([
                'success' => false,
                'error' => 'Validation failed',
                'messages' => $e->errors(),
            ], 422);
        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    public function gec_old_invoice_store(Request $request)
    {
        try {
            $checked = [];

            foreach ($request->all() as $item) {
                $validated = validator($item, [
                    'invoice_no' => 'nullable|string',
                    'invoice_date' => 'nullable|string',
                    'po_no' => 'nullable|string',
                    'po_date' => 'nullable|string',
                    'supplier_code' => 'nullable|string',
                    'buyer_code' => 'nullable|string',
                    'amount_excl_vat' => 'nullable|numeric',
                    'vat_amount' => 'nullable|numeric',
                    'amount_incl_vat' => 'nullable|numeric'
                ])->validate();

                $checked[] = $validated;
            }

            $output = [];

            $inserted = 0;
            $chunkSize = 1000; // ปรับตามขนาดข้อมูล/สเปกเครื่อง
            DB::beginTransaction();

            foreach (array_chunk($checked, $chunkSize) as $chunk) {
                // ถ้าไม่ต้องการ created_at/updated_at ให้เติมเองหรือปิด timestamps ใน model
                $now = now();
                foreach ($chunk as &$r) {
                    $r['created_at'] = $now;
                    $r['updated_at'] = $now;
                }
                // ใช้ตารางตรง ๆ จะไวกว่า create() ทีละแถว
                DB::table('gec_2023_invoices')->insert($chunk);
                $inserted += count($chunk);
            }

            DB::commit();
            // foreach ($checked as $item) {
            //     $gecInv = GecInvoice2023::create(
            //         $item
            //     );

            //     $output[] = $gecInv;
            // }

            return response()->json([
                'success' => true,
                'count_req' => count($request->all()),
                'count_output' => $inserted,
                'data' => $output,
            ]);
        } catch (ValidationException $e) {
            return response()->json([
                'success' => false,
                'error' => 'Validation failed',
                'messages' => $e->errors(),
            ], 422);
        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    public function dbd_supplier_store(Request $request)
    {
        try {
            $checked = [];

            foreach ($request->all() as $item) {

                if (isset($item['No'])) {
                    $item['gec_no'] = $item['No'];
                    unset($item['No']);
                }

                if (isset($item['group'])) {
                    $item['group_id'] = $item['group'];
                    unset($item['group']);
                }

                $validated = validator($item, [
                    'gec_no' => 'nullable|integer',
                    'registration_id' => 'required|string|max:20',
                    'supplier_id' => 'required|integer',
                    'is_supplier' => 'required|integer',
                    'start_effective_date' => 'nullable|date',
                    'size' => 'nullable|string|max:10',
                    'supplier_name' => 'required|string|max:255',
                    'registration_date' => 'nullable|date',
                    'registered_capital' => 'nullable|numeric',
                    'trade_receivables_net' => 'nullable|numeric',
                    'inventory' => 'nullable|numeric',
                    'current_assets' => 'nullable|numeric',
                    'property_plant_equipment' => 'nullable|numeric',
                    'non_current_assets' => 'nullable|numeric',
                    'total_assets' => 'nullable|numeric',
                    'current_liabilities' => 'nullable|numeric',
                    'non_current_liabilities' => 'nullable|numeric',
                    'total_liabilities' => 'nullable|numeric',
                    'shareholders_equity' => 'nullable|numeric',
                    'liabilities_and_equity' => 'nullable|numeric',
                    'group_id' => 'nullable|numeric',
                    'main_revenue' => 'nullable|numeric',
                    'total_revenue_fs' => 'nullable|numeric',
                    'cost_of_goods_sold' => 'nullable|numeric',
                    'gross_profit' => 'nullable|numeric',
                    'selling_and_admin_expenses' => 'nullable|numeric',
                    'total_expenses' => 'nullable|numeric',
                    'interest_expense' => 'nullable|numeric',
                    'profit_before_tax' => 'nullable|numeric',
                    'income_tax' => 'nullable|numeric',
                    'net_profit' => 'nullable|numeric',
                    'no_of_buyer' => 'nullable|integer',
                    'roa_percent' => 'nullable|numeric',
                    'roe_percent' => 'nullable|numeric',
                    'gross_profit_margin_percent' => 'nullable|numeric',
                    'operating_margin_percent' => 'nullable|numeric',
                    'net_margin_percent' => 'nullable|numeric',
                    'asset_turnover_ratio' => 'nullable|numeric',
                    'receivables_turnover_ratio' => 'nullable|numeric',
                    'inventory_turnover_ratio' => 'nullable|numeric',
                    'operating_expense_ratio' => 'nullable|numeric',
                    'current_ratio' => 'nullable|numeric',
                    'debt_to_asset_ratio' => 'nullable|numeric',
                    'asset_to_equity_ratio' => 'nullable|numeric',
                    'debt_to_equity_ratio' => 'nullable|numeric'
                ])->validate();

                $checked[] = $validated;
            }

            $output = [];

            foreach ($checked as $item) {
                $dbdSupplier = DbdSupplier::create(
                    // [
                    //     'invoice_no' => $item['invoice_no'],
                    //     'po_no' => $item['po_no']
                    // ],
                    $item
                );

                $output[] = $dbdSupplier;
            }

            return response()->json([
                'success' => true,
                'count_req' => count($request->all()),
                'count_output' => count($output),
                'data' => $output,
            ]);
        } catch (ValidationException $e) {
            return response()->json([
                'success' => false,
                'error' => 'Validation failed',
                'messages' => $e->errors(),
            ], 422);
        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    public function dbd_company_supplier_store(Request $request): JsonResponse
    {
        try {
            // 1) Validate ให้สอดคล้องกับ body ล่าสุด
            $data = $request->validate([
                'registration_number' => ['required', 'string', 'max:32'],
                'entity_type' => ['nullable', 'string', 'max:100'],

                // วันที่ใน body อาจมีทั้ง registered_date และ/หรือ incorporation_date_th
                'registered_date' => ['nullable', 'date'],
                'incorporation_date_th' => ['nullable', 'date'],

                'status' => ['nullable', 'string', 'max:100'],
                'registered_capital_baht' => ['nullable', 'numeric', 'min:0'],
                'address' => ['nullable', 'string'],

                'business_section_at_registration' => ['nullable', 'array'],
                'business_section_at_registration.code' => ['nullable', 'string', 'max:50'],
                'business_section_at_registration.description' => ['nullable', 'string'],

                'objective_at_registration' => ['nullable', 'string'],

                'business_section_latest' => ['nullable', 'array'],
                'business_section_latest.code' => ['nullable', 'string', 'max:50'],
                'business_section_latest.description' => ['nullable', 'string'],

                'objective_latest' => ['nullable', 'string'],

                'financial_filing_years_th' => ['nullable', 'array'],
                'financial_filing_years_th.*' => ['string', 'max:10'],

                // directors เป็น array ของ object { no, name }
                'directors' => ['nullable', 'array'],
                'directors.*.no'   => ['required_with:directors', 'integer', 'min:0'],
                'directors.*.name' => ['required_with:directors', 'string', 'max:255'],

                'title_card' => ['nullable', 'array'],
                'title_card.company_name' => ['nullable', 'string'],
                'title_card.entity_status' => ['nullable', 'string'],
                'title_card.business_size' => ['nullable', 'string', 'max:5'],
                'title_card.business_group' => ['nullable', 'string'],
            ]);

            // 2) Map → company_entity
            $mapped = $this->mapBodyToCompanyEntity($data);

            // 3) Transaction: upsert company + upsert business sections + sync directors
            return DB::transaction(function () use ($mapped, $data) {
                // upsert company_entity (key = registered_no)
                $company = CompanyEntity::updateOrCreate(
                    ['registered_no' => $mapped['registered_no']],
                    $mapped
                );

                // upsert master business sections (ถ้ามี code)
                $sections_upserted = 0;
                foreach (['business_section_at_registration', 'business_section_latest'] as $k) {
                    if (!empty($data[$k]['code'])) {
                        CompanyBusinessSection::updateOrCreate(
                            ['code' => trim($data[$k]['code'])],
                            ['description' => isset($data[$k]['description']) ? trim($data[$k]['description']) : null]
                        );
                        $sections_upserted++;
                    }
                }

                // sync directors → company_person (ลบทิ้งของบริษัทนี้ก่อนแล้วเติมใหม่)
                $directors_synced = 0;
                if (!empty($data['directors']) && is_array($data['directors'])) {
                    CompanyPerson::where('registered_no', $company->registered_no)->delete();

                    foreach ($data['directors'] as $d) {
                        $no   = isset($d['no']) ? (int)$d['no'] : null;
                        $name = (string)($d['name'] ?? '');

                        [$prefix, $first, $last] = $this->splitThaiName($name);
                        if ($first === '' && $last === '') {
                            continue;
                        }

                        CompanyPerson::create([
                            'registered_no' => $company->registered_no,
                            'citizen_id'    => null,
                            'prefix'        => $prefix,
                            'first_name'    => $first,
                            'last_name'     => $last,
                            'phone'         => null,
                            'is_owner'      => null,
                            'director_no'   => $no,   // <- จาก payload
                            'boj5_doc_no'   => null,
                        ]);
                        $directors_synced++;
                    }
                }

                return response()->json([
                    'ok' => true,
                    'mode' => $company->wasRecentlyCreated ? 'created' : 'updated',
                    'company_id' => $company->id,
                    'business_sections_upserted' => $sections_upserted,
                    'directors_synced' => $directors_synced,
                ], $company->wasRecentlyCreated ? 201 : 200);
            });
        } catch (ValidationException $e) {
            return response()->json([
                'success' => false,
                'error' => 'Validation failed',
                'messages' => $e->errors(),
            ], 422);
        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    private function mapBodyToCompanyEntity(array $src): array
    {
        $registeredCapital = isset($src['registered_capital_baht'])
            ? number_format((float)$src['registered_capital_baht'], 2, '.', '')
            : null;

        $status = isset($src['status']) ? trim($src['status']) : null;
        if ($status === 'ยังดำเนินกิจการอยู' && !empty($src['title_card']['entity_status'])) {
            $status = trim($src['title_card']['entity_status']);
        } elseif ($status === 'ยังดำเนินกิจการอยู') {
            $status = null;
        }

        $companyName = null;
        if (!empty($src['title_card']['company_name'])) {
            $companyName = trim($src['title_card']['company_name']);
        } elseif (!empty(trim($src['company_name']))) {
            $companyName = trim($src['company_name']);
        }

        return [
            'registered_no'                      => trim($src['registration_number']),
            'company_name_en'                    => null,
            'company_name_th'                    => $companyName,
            'entity_type_code'                   => isset($src['entity_type']) ? trim($src['entity_type']) : null,
            'registration_date'                  => $src['registered_date'] ?? ($src['incorporation_date_th'] ?? null),

            'business_group'                     => $src['title_card']['business_group'] ?? null,
            'company_status'                     => $status,
            'company_size'                       => $src['title_card']['business_size'] ?? null,

            'num_director'                       => isset($src['directors']) ? count($src['directors']) : null,
            'registered_capital_baht'            => $registeredCapital,
            'total_num_shares'                   => null,
            'value_per_share'                    => null,
            'is_hq'                              => true,
            'branch'                             => 'สำนักงานใหญ่',
            'registered_address'                 => $src['address'] ?? null,
            'address'                            => $src['address'] ?? null,
            'contact_person_id'                  => null,
            'vat_registered_no'                  => null,
            'is_vat'                             => null,
            'is_ncb'                             => null,
            'is_led'                             => null,
            'is_secured'                         => null,
            'business_section_registration_code' => $src['business_section_at_registration']['code'] ?? null,
            'business_section_latest_code'       => $src['business_section_latest']['code'] ?? null,
            'objective_at_registration'          => $src['objective_at_registration'] ?? null,
            'objective_latest'                   => $src['objective_latest'] ?? null,
        ];
    }

    /**
     * แยกคำนำหน้า-ชื่อ-นามสกุล (รองรับชื่อที่ติดกันและจุด)
     */
    private function splitThaiName(string $full): array
    {
        // ลบ zero-width และ normalize white space
        $s = preg_replace('/[\x{200B}\x{200C}\x{200D}\x{FEFF}]/u', '', $full);
        $s = trim(preg_replace('/\p{Z}+/u', ' ', $s));
        if ($s === '') return ['', '', ''];

        // คำนำหน้า (escape จุดด้วย \.)
        $honorifics = [
            'นาย',
            'นาง',
            'นางสาว',
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

    public function directorsByRegisteredNo(string $registered_no, Request $request): JsonResponse
    {
        $page = CompanyPerson::where('registered_no', $registered_no)
            ->orderByRaw('director_no IS NULL')  // ให้ NULL ไปท้าย
            ->orderBy('director_no')
            ->orderBy('id')
            ->paginate(50);

        $data = $page->getCollection()->map(function ($p) {
            return [
                'id'            => $p->id,
                'registered_no' => $p->registered_no,
                'director_no'   => $p->director_no,
                'prefix'        => $p->prefix,
                'first_name'    => $p->first_name,
                'last_name'     => $p->last_name,
            ];
        });

        $page->setCollection($data);

        return response()->json([
            'current_page' => $page->currentPage(),
            'data'         => $data,
            'total'        => $page->total(),
        ]);
    }

    public function getCompanyFinancial(string $tax_id, string $year, Request $request): JsonResponse
    {
        // --- 1) validate path params (เบา ๆ ที่ฝั่ง controller) ---
        $taxId = trim($tax_id);
        $fy    = (int) $year;

        if (!preg_match('/^\d{10,15}$/', $taxId)) {
            return response()->json([
                'ok' => false,
                'error' => 'Invalid tax_id format.',
            ], 422);
        }
        if ($fy < 1990 || $fy > 2100) {
            return response()->json([
                'ok' => false,
                'error' => 'Invalid fiscal year.',
            ], 422);
        }

        // --- 2) fetch จาก 3 ตาราง (ตัวละ 1 แถวตามปี) ---
        $bs = CompanyBalanceSheet::where('tax_id', $taxId)->where('fiscal_year', $fy)->first();
        $is = CompanyIncomeStatement::where('tax_id', $taxId)->where('fiscal_year', $fy)->first();
        $rt = CompanyFinancialRatios::where('tax_id', $taxId)->where('fiscal_year', $fy)->first();

        if (!$bs && !$is && !$rt) {
            return response()->json([
                'ok' => false,
                'error' => 'No financial data found for given tax_id/year.',
            ], 404);
        }

        // --- 3) แปลงเป็น object ที่สะอาด: เอาเฉพาะคอลัมน์ตัวเลขของรายงาน ---
        $balance = $bs ? $this->onlyCols($bs->toArray(), [
            'accounts_receivable_net',
            'inventories',
            'current_assets',
            'property_plant_equipment',
            'non_current_assets',
            'total_assets',
            'current_liabilities',
            'non_current_liabilities',
            'total_liabilities',
            'shareholders_equity',
            'total_liabilities_and_shareholder_equity',
        ]) : null;

        $income = $is ? $this->onlyCols($is->toArray(), [
            'net_revenue',
            'total_revenue',
            'cost_of_goods_sold',
            'gross_profit',
            'operating_expenses',
            'total_expenses',
            'interest_expenses',
            'profit_before_tax',
            'income_tax_expenses',
            'net_profit',
        ]) : null;

        $ratios = $rt ? $this->onlyCols($rt->toArray(), [
            'return_on_assets_percent',
            'return_on_equity_percent',
            'gross_profit_margin_percent',
            'operating_profit_margin_percent',
            'net_profit_margin_percent',
            'current_ratio_times',
            'accounts_receivable_turnover_times',
            'inventory_turnover_times',
            'accounts_payable_turnover_times',
            'total_asset_turnover_times',
            'operating_expense_ratio_percent',
            'total_assets_to_shareholders_equity_ratio_times',
            'total_liabilities_to_total_assets_ratio_times',
            'debt_to_equity_ratio_times',
            'debt_to_working_capital_ratio_times',
        ]) : null;

        // --- 4) response ---
        return response()->json([
            'ok'          => true,
            'tax_id'      => $taxId,
            'fiscal_year' => $fy,
            'balance'     => $balance,
            'income'      => $income,
            'ratios'      => $ratios,
        ]);
    }

    /**
     * คืน array ที่มีเฉพาะคีย์ที่กำหนด และแปลงค่าว่างให้เป็น null (ถ้าอยาก)
     */
    private function onlyCols(array $row, array $cols): array
    {
        $out = [];
        foreach ($cols as $c) {
            if (array_key_exists($c, $row)) {
                // แปลงเป็น float/null เพื่อความสม่ำเสมอของ API
                $out[$c] = is_null($row[$c]) ? null : (float) $row[$c];
            } else {
                $out[$c] = null;
            }
        }
        return $out;
    }

    public function getCompanyFinancialAllYears(string $tax_id, Request $request): \Illuminate\Http\JsonResponse
    {
        $taxId = trim($tax_id);
        if (!preg_match('/^\d{10,15}$/', $taxId)) {
            return response()->json(['ok' => false, 'error' => 'Invalid tax_id format.'], 422);
        }

        // optional filter: ?from=2020&to=2024
        $from = $request->integer('from');
        $to   = $request->integer('to');

        $bsQ = CompanyBalanceSheet::where('tax_id', $taxId);
        $isQ = CompanyIncomeStatement::where('tax_id', $taxId);
        $rtQ = CompanyFinancialRatios::where('tax_id', $taxId);

        if ($from) {
            $bsQ->where('fiscal_year', '>=', $from);
            $isQ->where('fiscal_year', '>=', $from);
            $rtQ->where('fiscal_year', '>=', $from);
        }
        if ($to) {
            $bsQ->where('fiscal_year', '<=', $to);
            $isQ->where('fiscal_year', '<=', $to);
            $rtQ->where('fiscal_year', '<=', $to);
        }

        $bsRows = $bsQ->orderBy('fiscal_year')->get();
        $isRows = $isQ->orderBy('fiscal_year')->get();
        $rtRows = $rtQ->orderBy('fiscal_year')->get();

        if ($bsRows->isEmpty() && $isRows->isEmpty() && $rtRows->isEmpty()) {
            return response()->json(['ok' => false, 'error' => 'No financial data found for given tax_id.'], 404);
        }

        // คีย์ที่จะดึงใน response
        $bsCols = [
            'accounts_receivable_net',
            'inventories',
            'current_assets',
            'property_plant_equipment',
            'non_current_assets',
            'total_assets',
            'current_liabilities',
            'non_current_liabilities',
            'total_liabilities',
            'shareholders_equity',
            'total_liabilities_and_shareholder_equity'
        ];
        $isCols = [
            'net_revenue',
            'total_revenue',
            'cost_of_goods_sold',
            'gross_profit',
            'operating_expenses',
            'total_expenses',
            'interest_expenses',
            'profit_before_tax',
            'income_tax_expenses',
            'net_profit'
        ];
        $rtCols = [
            'return_on_assets_percent',
            'return_on_equity_percent',
            'gross_profit_margin_percent',
            'operating_profit_margin_percent',
            'net_profit_margin_percent',
            'current_ratio_times',
            'accounts_receivable_turnover_times',
            'inventory_turnover_times',
            'accounts_payable_turnover_times',
            'total_asset_turnover_times',
            'operating_expense_ratio_percent',
            'total_assets_to_shareholders_equity_ratio_times',
            'total_liabilities_to_total_assets_ratio_times',
            'debt_to_equity_ratio_times',
            'debt_to_working_capital_ratio_times'
        ];

        $balance = [];
        foreach ($bsRows as $r) {
            $balance[(string)$r->fiscal_year] = $this->onlyCols($r->toArray(), $bsCols);
        }

        $income = [];
        foreach ($isRows as $r) {
            $income[(string)$r->fiscal_year] = $this->onlyCols($r->toArray(), $isCols);
        }

        $ratios = [];
        foreach ($rtRows as $r) {
            $ratios[(string)$r->fiscal_year] = $this->onlyCols($r->toArray(), $rtCols);
        }

        return response()->json([
            'ok'     => true,
            'tax_id' => $taxId,
            'balance' => $balance,
            'income'  => $income,
            'ratios'  => $ratios,
            'range'   => ['from' => $from, 'to' => $to],
        ]);
    }
}
