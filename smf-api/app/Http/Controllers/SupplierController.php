<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;

class SupplierController extends Controller
{
    public function get_supplier_info(Request $request)
    {

        $id = $request->route('id');


        return response()->json([
            'supplier_id' => $id,
            'message' => "Supplier info for ID {$id}"
        ]);
    }
}
