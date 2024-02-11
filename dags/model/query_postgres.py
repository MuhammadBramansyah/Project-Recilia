
def channelTable():
    ## query untuk semenrtara
    sql = """
    select name,id,code
    from channels
   
    """
    return sql

def partnerTable():
    ## query untuk sementara
    sql = """
    select distinct name,id,code
    from partners
    
    """
    return sql

def listChannelPartner():
    tables = [("channel", channelTable()),
              ("partner", partnerTable())]
    
    return tables

def get_channel_id(channel_codes):
    ## query untuk semenrtara
    sql = f"""
    select id
    from channels c 
    where code = '{channel_codes}';
    """
    return sql

def get_email():
    return """
        with channel as (
                select a.id,
                    a.email,
                    b.code,
                    a.is_bo
                from users as a
                inner join user_channels as c 
                on a.id = c.user_id 
                inner join channels as b 
                on b.id = c.channel_id 
                where a.deleted_at is null and b.deleted_at is null 
                -- and  a.deactivated_at is null
                ),
            partner as (
                    select a.id,
                        a.email,
                        b.code,
                        a.is_partner
                from users as a
                inner join user_partners as c 
                on a.id = c.user_id 
                inner join partners as b 
                on b.id = c.partner_id 
                where a.deleted_at is null and b.deleted_at is null	
                -- and  a.deactivated_at is null
                ),
            channel_partner as (
                select a.id as channel_id,
                    b.id as partner_id,
                    a.code as channel_code,
                    b.code as partner_code,
                    a.email as channel_email,
                    b.email as partner_email,
                    a.is_bo,
                    b.is_partner
                from channel as a
                inner join partner as b 
                on a.id = b.id 
                ) 
    """

def query_reminder(start_date): ## trx logs
    return f""" 
    with logs as (
        select distinct tl.upload_date, u.email , c.code as channel_code, p.code partner_code, u.is_bo, u.is_partner
        from transaction_logs tl
            join users u on tl.uploader_id = u.id
            join channels c on c.id = tl.channel_id
		    join partners p on p.id = tl.partner_id 
            where tl.deleted_at is null and 
            c.deleted_at is null and 
            p.deleted_at is null and 
            u.deleted_at is null
    )select * from logs where upload_date= '{start_date}'
    """

def update_trx_logs(id):
    return f""" 
   update transaction_logs 
   set status = 'R',pool_date = null, total_rows =0, skipped_rows = 0
   where id in {id}
    """

def query_check_logs(): ## trx logs
    return f""" 
    with logs as (
        select distinct tl.id, tl.created_at, tl.source, c.code as channel_code, p.code partner_code
        from transaction_logs tl
            join channels c on c.id = tl.channel_id
		    join partners p on p.id = tl.partner_id 
            where tl.deleted_at is null and tl.status = 'F' and
            c.deleted_at is null and 
            p.deleted_at is null 
    )
    """