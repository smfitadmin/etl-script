<?php

// database/migrations/2025_08_27_000000_create_gec_inv_2023_table.php
use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('gec_inv_2023', function (Blueprint $table) {
            $table->bigIncrements('id');

            $table->string('invoice_no', 100)->nullable()->index();
            $table->date('invoice_date')->nullable()->index();

            $table->string('po_no', 100)->nullable()->index();
            $table->date('po_date')->nullable();

            $table->string('supplier_code', 64)->nullable()->index();
            $table->string('buyer_code', 64)->nullable()->index();

            $table->decimal('amount_excl_vat', 15, 2)->nullable();
            $table->decimal('vat_amount', 15, 2)->nullable();
            $table->decimal('amount_incl_vat', 15, 2)->nullable();

            // $table->string('source_sheet', 64)->nullable();

            $table->timestamps();

            // ถ้าต้องการกันซ้ำ: เปิดอันนี้เมื่อมั่นใจคีย์
            // $table->unique(['invoice_no', 'buyer_code']);
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('gec_inv_2023');
    }
};
