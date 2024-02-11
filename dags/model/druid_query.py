def main_query(date):
    return f""" 
    SELECT 
      TIME_FLOOR(__time, 'P1D') as ZDATE,

      partner_channel_code AS ZBRANDAPPS,
      
      "partner_partner_code" AS ZMERCHANT,

      LOOKUP(CONCAT(partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', ebs_payment_method), 'payment_method') AS payment_method,

      LOOKUP(partner_channel_code, 'vendor_product') AS ZVENDPROD,

      LOOKUP(partner_channel_code, 'vendor_delivery') as ZVENDDELIV,

      sum(titipan_product_amount - (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee')AS DOUBLE))) as ZTP_NETPRODUCT,

      sum(shipping_delivery_orbit_amount) as ZTS1_NETPRODUCT,

      sum(shipping_delivery_toos_amount) as ZTS2_TOTAL,
      
      sum(shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)) as ZTS2_MDRDPP,

      sum(shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) * 0.11) as ZTS2_MDRPPN,

      SUM(CAST(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_pph')AS DOUBLE)) as ZTS2_MDRPPH,

      sum(shipping_delivery_toos_amount 
        - (shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)) 
        - (shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) * 0.11) 
        +CAST(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_pph')AS DOUBLE)) as ZTS2_NETPRODUCT,

      sum(discount_amount) as ZDISC_TOTAL,

      sum(refund_amount) as ZREFUND_TOTAL,

        -- kenapa beda rumus??    
      sum((sales_amount-refund_amount-discount_amount) / CAST(LOOKUP(partner_channel_code, 'sales_dpp_by_channel_code') AS DOUBLE)) AS ZNETSALES_DPP,

        --kenapa beda rumus?
    --   sum((sales_amount / CAST(LOOKUP(partner_channel_code, 'sales_dpp_by_channel_code') AS DOUBLE)) *CAST(LOOKUP(partner_channel_code, 'sales_ppn_by_channel_code') AS DOUBLE)) AS ZNETSALES_PPN, 

      sum(((sales_amount-refund_amount-discount_amount) / CAST(LOOKUP(partner_channel_code, 'sales_dpp_by_channel_code') AS DOUBLE)) 
      * CAST(LOOKUP(partner_channel_code, 'sales_ppn_by_channel_code') AS DOUBLE)) AS ZNETSALES_PPN,
      
        -- need reconfirm with the old code
      sum(sales_amount-refund_amount-discount_amount) AS ZNETSALES_TOTAL,
      
      sum(case
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA' then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ' then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'CC' then cc_tanpa_cicilan_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'VAQTY' then (va_bca_qty+va_bni_qty+va_mandiri_qty+va_bri_qty+va_permata_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
                else 0
            end) AS ZMDRFEE_DPP,
     
      sum((case
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA' then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ' then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'CC' then cc_tanpa_cicilan_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'VAQTY' then (va_bca_qty+va_bni_qty+va_mandiri_qty+va_bri_qty+va_permata_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
                else 0
            end) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_ppn') AS DOUBLE)) AS ZMDRFEE_PPN,

      sum((case
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA' then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ' then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'CC' then cc_tanpa_cicilan_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
                when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'VAQTY' then (va_bca_qty+va_bni_qty+va_mandiri_qty+va_bri_qty+va_permata_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
                else 0
            end) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_ppn') AS DOUBLE) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_pph') AS DOUBLE)) AS ZMDRFEE_PPH,

      sum(0) as ZMDRFEE_PPH_FLAG,

      sum(0) as ZTSEL_SHIP,
     
      sum("other_income") AS ZOTHER,
      
      sum((sales_amount-refund_amount-discount_amount) 
    - (case
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA' then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ' then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'CC' then cc_tanpa_cicilan_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'VAQTY' then (va_bca_qty+va_bni_qty+va_mandiri_qty+va_bri_qty+va_permata_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
        else 0
        end) 
    - ((case
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA' then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ' then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'CC' then cc_tanpa_cicilan_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'VAQTY' then (va_bca_qty+va_bni_qty+va_mandiri_qty+va_bri_qty+va_permata_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
        else 0
        end) 
    * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_ppn') AS DOUBLE)) 
    - ((case
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA' then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ' then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'CC' then cc_tanpa_cicilan_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
        when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet_remarks') = 'VAQTY' then (va_bca_qty+va_bni_qty+va_mandiri_qty+va_bri_qty+va_permata_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term, '|', channel_payment_method), 'mdr_dpp_tsel_finnet') AS DOUBLE)
        else 0
        end) 
    * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_ppn') AS DOUBLE) 
    *cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_pph') AS DOUBLE)) 
    -(case
        when partner_channel_code='ORBIT0' then shipping_delivery_orbit_amount
        else shipping_delivery_toos_amount - (shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_titipan_shipping_delivery') AS DOUBLE)) - (shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_titipan_shipping_delivery') AS DOUBLE) *CAST(LOOKUP(partner_channel_code, 'sales_ppn_by_channel_code') AS DOUBLE))
        end) 
    - ((titipan_product_amount - (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE))) 
    - ((cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE) 
    * (titipan_product_amount - (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE)) 
    + shipping_delivery_orbit_amount + (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE)))) 
    *CAST(LOOKUP(partner_channel_code, 'sales_ppn_by_channel_code') AS DOUBLE)) 
    - (cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE) 
    * (titipan_product_amount - (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE)) 
    + shipping_delivery_orbit_amount + (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE)))))) AS ZNET_SETTLEMENT,
      
      sum(ebs_charges * cast(lookup(partner_partner_code, 'finnet_ebs_charges') AS DOUBLE)) AS ZCHARGES,
     
      sum("incoming_fund") AS ZINCOMING_FUND,

      LOOKUP(CONCAT(channel_code, '|', partner_code), 'account_number') AS ZREKBANK
    
     
FROM rp_match_daily
WHERE TIME_FLOOR(__time, 'P1D') = TIMESTAMP '{date}' 

GROUP BY 
    1,
    "partner_channel_code",
    "partner_partner_code",
    LOOKUP(CONCAT(partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', ebs_payment_method), 'payment_method'),
    LOOKUP(CONCAT(channel_code, '|', partner_code), 'account_number'),
    LOOKUP(partner_channel_code, 'vendor_product') ,
    LOOKUP(partner_channel_code, 'vendor_delivery')
    """

def get_partner_processed(date):
    return f""" 
        select partner_processed from rp_match_daily
        WHERE TIME_FLOOR(__time, 'P1D') = TIMESTAMP '{date}'  
    """

## testing purpose
def match_query_test(dates):
    return f""" 
        with abc as (    
            select 
            -- get date
            TIME_FLOOR(__time, 'P1D') as ZDATE,

            -- get channel
            partner_channel_code as ZBRANDAPPS,

            --get partner
            partner_partner_code as ZMERCHANT,

            --get vendor product
            LOOKUP(partner_channel_code, 'vendor_product') AS ZVENDPROD,

            -- get vendor deliv
            LOOKUP(partner_channel_code, 'vendor_delivery') as ZVENDDELIV,

            -- get net product
            sum(titipan_product_amount - (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee')AS DOUBLE))) as ZTP_NETPRODUCT,

            -- get net porduct orbit
            sum(shipping_delivery_orbit_amount) as ZTS1_NETPRODUCT,

            -- get titipan shipping toos
            sum(shipping_delivery_toos_amount) as ZTS2_TOTAL,

            -- get mdr_dpp toos
            sum(shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)) as ZTS2_MDRDPP,

            -- get mdr_ppn toos
            sum(shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) * 0.11) as ZTS2_MDRPPN,

            -- get mdr_pph toos
            SUM(CAST(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_pph')AS DOUBLE)) as ZTS2_MDRPPH,

            -- get net titipan shipping delivery
            sum(shipping_delivery_toos_amount - (shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)) - (shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) * 0.11) +CAST(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_pph')AS DOUBLE)) as ZTS2_NETPRODUCT,

            -- get total discount
            sum(discount_amount) as ZDISC_TOTAL,

            -- get total refund
            sum(refund_amount) as ZREFUND_TOTAL,

            -- get net_sales dpp
            sum((sales_amount - discount_amount - refund_amount)/1.11) as ZNETSALES_DPP,

            -- get net_sales_ppn
            sum(((sales_amount - discount_amount - refund_amount)/1.11) * 0.11) as ZNETSALES_PPN,

            -- get total netsales
            sum(sales_amount - discount_amount - refund_amount) as ZNETSALES_TOTAL,

            -- get mdr_dpp
            sum(case
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA' 
                        then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ' 
                        then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' 
                        then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' 
                        then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' 
                        then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' 
                        then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    else 0
                end) AS ZMDRFEE_DPP,

            -- get mdr_ppn
            sum((case
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA' 
                        then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
            
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ' 
                        then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' 
                        then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' 
                        then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' 
                        then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' 
                        then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    else 0
                end) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_ppn') AS DOUBLE)) AS ZMDRFEE_PPN,
            
            -- get mdr_pph
            sum((case
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSA'
                        then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'NSQ'
                        then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SA' 
                        then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'SQ' 
                        then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PA' 
                        then package_amount * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp_remarks') = 'PQ' 
                        then package_qty * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code, '|', partner_payment_type, '|', partner_payment_method, '|', partner_acquiring_bank, '|', partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    else 0
                end) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_ppn') AS DOUBLE) * cast(LOOKUP(CONCAT(partner_channel_code, '|', partner_partner_code), 'mdr_pph') AS DOUBLE)) AS ZMDRFEE_PPH,
                
                -- get mdr_pph_flag
                sum(0) as ZMDRFEE_PPH_FLAG,

                -- get shipping delivery (tsel)

                --get 
                sum(0) as ZTSEL_SHIP,

                -- get other income
                sum(0) as ZOTHER,
                
                -- get netsettlement
                sum((sales_amount-refund_amount-discount_amount) -
                (case 
                    when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'NSA' 
                    then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)
                
                    when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'NSQ'
                    then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'SA'
                    then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'SQ'
                    then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'PA'
                    then package_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)
                    
                    when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'PQ'
                    then package_qty * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE)
                
                else 0 
            end) - 
            ((case 
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'NSA' 
                then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'NSQ'
                then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'SA'
                then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'SQ'
                then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'PA'
                then package_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'PQ'
                then package_qty * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                else 0 
            end) 
            * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_ppn') AS DOUBLE)) -
            ((case 
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'NSA' 
                then (sales_amount - discount_amount - refund_amount) * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'NSQ'
                then (sales_qty - refund_qty) * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'SA'
                then sales_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'SQ'
                then sales_qty * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'PA'
                then package_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                when LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term),'mdr_dpp_remarks') = 'PQ'
                then package_qty * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) 
                
                else 0 
            end) 
            * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_ppn') AS DOUBLE) * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_pph') AS DOUBLE))
            - (case
                when partner_channel_code='ORBIT0' then shipping_delivery_orbit_amount
                else shipping_delivery_toos_amount  - (shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_titipan_shipping_delivery') AS DOUBLE)) - (shipping_delivery_toos_amount * cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code), 'mdr_titipan_shipping_delivery') AS DOUBLE)  *CAST(LOOKUP(partner_channel_code, 'sales_ppn_by_channel_code') AS DOUBLE))
            end) - 
            ((titipan_product_amount - (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE))) 
            - (( cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) * 
            (titipan_product_amount - (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE) ) + shipping_delivery_orbit_amount + (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE)))) *
            CAST(LOOKUP(partner_channel_code, 'sales_ppn_by_channel_code') AS DOUBLE)) - ( cast(LOOKUP(CONCAT(partner_channel_code,'|',partner_partner_code,'|',partner_payment_type,'|',partner_payment_method,'|',partner_acquiring_bank,'|',partner_installment_term), 'mdr_dpp') AS DOUBLE) *  
            (titipan_product_amount - (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE) ) + shipping_delivery_orbit_amount + (titipan_product_qty * CAST(LOOKUP(partner_channel_code, 'orbit_bundling_fee') AS DOUBLE)))))) as ZNET_SETTLEMENT,
            
            -- get charges
            case 
                when partner_partner_code = 'FINNET' OR partner_partner_code = 'FINPAY' OR partner_partner_code = 'FINNVA'
                OR partner_partner_code = 'FINNCC' THEN count(incoming_fund) * cast(lookup(partner_partner_code,'finnet_ebs_charges') AS  DOUBLE) 
            else 0 
            end as ZCHARGES,

            -- get incoming fund
            sum(incoming_fund) as ZINCOMING_FUND,

            -- get Rekening Bank
            -- account_number as ZREKBANK,
            
            -- get Keterangan
            case 
                when TIME_FLOOR(__time, 'P1D') != partner_processed then concat('Adjustment : ',TIME_FORMAT(__time,'yyyy-MM-dd'))
                else ''
            end as ZKETERANGAN,

            -- get processd time
            partner_processed
                
            from rp_match_daily
            WHERE TIME_FLOOR(__time, 'P1D') = TIMESTAMP '{dates}'
            group by 1,
                    2,
                    3,
                    -- account_number,
                    partner_processed,
                    __time
            )
            select 
            ZDATE,
            ZBRANDAPPS,
            ZMERCHANT,
            ZVENDPROD,
            ZVENDDELIV,
            ZTP_NETPRODUCT,
            ZTS1_NETPRODUCT,
            ZTS2_TOTAL,
            ZTS2_MDRDPP,
            ZTS2_MDRPPN,
            ZTS2_MDRPPH,
            ZTS2_NETPRODUCT,
            ZDISC_TOTAL,
            ZREFUND_TOTAL,
            ZNETSALES_DPP,
            ZNETSALES_PPN,
            ZNETSALES_TOTAL,
            ZMDRFEE_DPP,
            ZMDRFEE_PPN,
            ZMDRFEE_PPH,
            ZMDRFEE_PPH_FLAG,
            ZTSEL_SHIP,
            ZOTHER,
            ZNET_SETTLEMENT,
            ZCHARGES,
            ZINCOMING_FUND,
            -- ZREKBANK,
            ZKETERANGAN
            from abc  
       """ 
        
 
    

## querying get data h-1 saat dag running
### EX: dag running hr ini maka get data kemarin
def match_query(start_date):
    return f"""
            select
            DISTINCT
                    __time as day_month_year_x,
                    channel_channel_code as channel_code_x,
                    channel_partner_code as partner_code_x, 
                    channel_transaction_id as transaction_id,
                    channel_transaction_amount as transaction_amount_x, 
                    recon_status as recon_status_x,
                    __time as day_month_year_y, 
                    partner_channel_code as channel_code_y,
                    partner_partner_code as partner_code_y,
                    partner_transaction_id as transaction_id_r, 
                    partner_transaction_amount as transaction_amount_y,
                    recon_status as recon_status_y
                    
            from rp_merge
            where recon_status = 'Match' AND TIME_FLOOR(__time, 'P1D') = TIMESTAMP '{start_date}'
            // __time >= TIMESTAMP '{start_date}' AND __time < TIMESTAMP '{end_date}' 
    """

def unmatch_query(start_date):
    return f"""
            select
            DISTINCT
                __time as day_month_year_x, 
                channel_channel_code as channel_code_x,
                channel_partner_code as partner_code_x, 
                channel_transaction_id as transaction_id,
                channel_transaction_amount as transaction_amount_x, 
                recon_status as recon_status_x,
                __time as day_month_year_y, 
                partner_channel_code as channel_code_y,
                partner_partner_code as partner_code_y,
                partner_transaction_id as transaction_id_r, 
                partner_transaction_amount transaction_amount_y,
                partner_refund_amount as refund_amount,
                partner_payment_method as payment_method,
                partner_payment_type as payment_type,
                recon_status as recon_status_y
                
            from rp_merge
            where recon_status = 'Unmatch' AND TIME_FLOOR(__time, 'P1D') = TIMESTAMP '{start_date}'
    """

def channel_partner_code_druid(dates):
    return f""" 
        select
            DISTINCT
                    __time,
                    partner_channel_code as channel_code,
                    partner_partner_code as partner_code
                    from rp_merge
                    where TIME_FLOOR(__time, 'P1D') = TIMESTAMP '{dates}'
        """

def channel_partner_code_druid(start_date):
    return f""" 
        select
            DISTINCT
                    __time,
                    partner_channel_code as channel_code,
                    partner_partner_code as partner_code
                    from rp_merge
                    where __time = timestamp '{start_date}'
                    -- and "partner_channel_code" = 'MYTSEL'
        """

def channel_partner_code_druid(start_date):
    return f""" 
        select
            DISTINCT
                    __time,
                    partner_channel_code as channel_code,
                    partner_partner_code as partner_code
                    from rp_merge
                    where __time = timestamp '{start_date}'
        """