def q_partner_match(dates):
    return f""" 
        select 
            transaction_date,
            day_month_year,
            channel_code,
            partner_code,
            transaction_id,
            transaction_amount,
            payment_type,
            refund_amount,
            payment_method,
            recon_status
        from partner  
        where day_month_year = '{dates}' and recon_status = 1
        ALLOW FILTERING;
    """

def q_channel_match(dates):
    return f""" 
        select 
            transaction_date,
            day_month_year,
            channel_code,
            partner_code,
            transaction_id,
            transaction_amount,
            recon_status
        from channel  
        where day_month_year = '{dates}' and recon_status = 1
        ALLOW FILTERING; 
    """

def q_partner_unmatch(dates):
    return f""" 
        select 
            transaction_date,
            day_month_year,
            channel_code,
            partner_code,
            transaction_id,
            transaction_amount,
            payment_type,
            refund_amount,
            payment_method,
            recon_status
        from partner  
        where day_month_year = '{dates}' and recon_status = 2
        ALLOW FILTERING;
    """

def q_channel_unmatch(dates):
    return f""" 
        select 
            transaction_date,
            day_month_year,
            channel_code,
            partner_code,
            transaction_id,
            transaction_amount,
            recon_status
        from channel  
        where day_month_year = '{dates}' and recon_status = 2
        ALLOW FILTERING; 
    """