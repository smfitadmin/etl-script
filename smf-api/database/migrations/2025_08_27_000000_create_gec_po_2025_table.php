<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('gec_po_2025', function (Blueprint $table) {
            $table->bigIncrements('id');

            $table->string('supplier_name', 100)->nullable()->index();

            $table->string('po_no', 100)->nullable()->index();
            $table->date('po_date')->nullable();

            $table->decimal('amount_excl_vat', 15, 2)->nullable();
            $table->decimal('vat_amount', 15, 2)->nullable();
            $table->decimal('amount_incl_vat', 15, 2)->nullable();

            $table->date('po_shipment_date')->nullable();
            $table->integer('po_payment_term')->nullable()->index();

            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('gec_po_2025');
    }
};
