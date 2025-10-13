<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use App\Models\BolBsRaw;
use App\Models\BolIcRaw;
use App\Models\GecInvoice;
use App\Models\GecPurchaseOrder;
use App\Models\DbdSupplier;
use App\Models\GecInvoice2023;

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
}
