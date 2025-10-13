<?php

use Illuminate\Support\Facades\Route;
use App\Http\Controllers\AuthController;
use App\Http\Controllers\PublicApiController;
use App\Http\Controllers\SupplierController;
use Illuminate\Http\Request;

Route::post('/login', [AuthController::class, 'login']);

Route::get('/ping', function () {
    return response()->json([
        'message' => 'Laravel API is working!',
        'status' => 'ok'
    ]);
});

Route::prefix('/public')->group(function () {
    Route::post('/bol-bs', [PublicApiController::class, 'bol_bs_store']);
    Route::post('/bol-ic', [PublicApiController::class, 'bol_ic_store']);

    Route::post('/gec-inv', [PublicApiController::class, 'gec_invoice_store']);
    Route::post('/gec-po', [PublicApiController::class, 'gec_po_store']);

    Route::post('/gec-old-inv', [PublicApiController::class, 'gec_old_invoice_store']);

    Route::post('/dbd-supplier', [PublicApiController::class, 'dbd_supplier_store']);
});

Route::middleware('auth:sanctum')->group(function () {
    Route::get('/user', function (Request $request) {
        return $request->user();
    });
    Route::prefix('/supplier')->group(function () {
        Route::get('/{id}', [SupplierController::class, 'get_supplier_info']);
    });
});
