import pandas as pd
from DBconnections import Credential

class ETL:
    def __init__(self):
        self.__credential = Credential()


    def fetch_active_etl_table(self):
        df_etl_table = pd.read_csv('etl_config.csv')
        df_active_etl = df_etl_table[df_etl_table['is_active'] == 1]
        return df_active_etl

    def full_etl_process(self):
        df_active_etl = self.fetch_active_etl_table()
        df_active_etl = df_active_etl[df_active_etl['refresh_type'] == 'FULL']
        con_to_live = self.__credential.live_connection()
        live_cursor = con_to_live.cursor()
        con_to_dwh = self.__credential.dwh_connection_engine()
        num_of_table = df_active_etl.shape[0]
        for no_t in range(0, num_of_table):
            sql = '''
            select * from '''+df_active_etl.iloc[no_t]['source_db']+'''.'''+df_active_etl.iloc[no_t]['source_table']+''';
            '''
            print(sql)
            live_cursor.execute(sql)
            record = live_cursor.fetchall()
            field_names = [i[0] for i in live_cursor.description]
            source_df = pd.DataFrame.from_records(record, columns=field_names)
            source_df.to_sql(df_active_etl.iloc[no_t]['dest_table'], schema='public', con=con_to_dwh, if_exists='replace',
                             index=False, method='multi', chunksize=10000)


    def incremental_etl_process(self):
        df_active_etl = self.fetch_active_etl_table()
        df_active_etl = df_active_etl[df_active_etl['refresh_type'] == 'INCREMENTAL']
        con_to_live = self.__credential.live_connection()
        live_cursor = con_to_live.cursor()
        con_to_dwh = self.__credential.dwh_connection_engine()
        num_of_table = df_active_etl.shape[0]
        for no_t in range(0, num_of_table):

            hour  = '''select  EXTRACT(hour from now())'''
            live_cursor.execute(hour)
            hour = live_cursor.fetchall()[0][0]
            print('Hour' ,hour)
            if hour != 0:
                sql = '''
                select * from ''' + df_active_etl.iloc[no_t]['source_db'] + '''.''' + df_active_etl.iloc[no_t]['source_table'] + '''
                where 1=1
                and DATE('''+df_active_etl.iloc[no_t]['time_ind_col']+''') = CURRENT_DATE() 
                and EXTRACT(HOUR FROM '''+df_active_etl.iloc[no_t]['time_ind_col']+''') = EXTRACT(HOUR FROM NOW())-1
                ;
                '''
            else:
                sql = '''
                select * from ''' + df_active_etl.iloc[no_t]['source_db'] + '''.''' +df_active_etl.iloc[no_t]['source_table'] + '''
                where 1=1
                and DATE(''' + df_active_etl.iloc[no_t]['time_ind_col'] + ''') = CURRENT_DATE()-1 
                and EXTRACT(HOUR FROM ''' + df_active_etl.iloc[no_t]['time_ind_col'] + ''') = 23
                ;
                '''
            print(sql)

            live_cursor.execute(sql)
            records = live_cursor.fetchall()
            field_names = [i[0] for i in live_cursor.description]
            source_df = pd.DataFrame.from_records(records, columns=field_names)
            source_df.to_sql(df_active_etl.iloc[no_t]['dest_table'], schema='public', con=con_to_dwh,
                             if_exists='append', index=False, method='multi', chunksize=10000)

    def update_existing_tables(self):
        df_active_etl = self.fetch_active_etl_table()
        df_active_etl = df_active_etl[df_active_etl['refresh_type'] == 'INCREMENTAL']
        con_to_live = self.__credential.live_connection()
        live_cursor = con_to_live.cursor()
        con_to_dwh = self.__credential.dwh_connection_engine()
        dwh_cursor = con_to_dwh.connect()
        num_of_table = df_active_etl.shape[0]
        for no_t in range(0, num_of_table):
            hour = '''select  EXTRACT(hour from now())'''
            live_cursor.execute(hour)
            hour = live_cursor.fetchall()[0][0]
            print('Hour', hour)
            if hour != 0:
                sql = '''
                select GROUP_CONCAT(''' + df_active_etl.iloc[no_t]['primary_key_col'] + ''')  
                from ''' + df_active_etl.iloc[no_t]['source_db'] + '''.''' + df_active_etl.iloc[no_t]['source_table'] + '''
                where 1=1
                and extract(hour from ''' + df_active_etl.iloc[no_t]['update_col'] + ''') =  EXTRACT(HOUR FROM NOW())-1
                and date(''' + df_active_etl.iloc[no_t]['update_col'] + ''') = current_date();
                '''
            else:
                sql = '''
                select GROUP_CONCAT(''' + df_active_etl.iloc[no_t]['primary_key_col'] + ''')  
                from ''' + df_active_etl.iloc[no_t]['source_db'] + '''.''' + df_active_etl.iloc[no_t][
                'source_table'] + '''
                where 1=1
                and extract(hour from ''' + df_active_etl.iloc[no_t]['update_col'] + ''') = 23
                and date(''' + df_active_etl.iloc[no_t]['update_col'] + ''') = current_date()-1;
                                '''
            live_cursor.execute(sql)
            records = live_cursor.fetchall()[0][0]
            if records is not None:
                sql = '''
                        select *
                        from ''' + df_active_etl.iloc[no_t]['source_db'] + '''.''' + df_active_etl.iloc[no_t]['source_table'] + '''
                        where 1=1
                        and ''' + df_active_etl.iloc[no_t]['primary_key_col'] + '''  in ('''+records+''') ;
                        '''
                print(sql)
                live_cursor.execute(sql)
                updated_records = live_cursor.fetchall()
                del_sql = '''
                    delete from ''' + df_active_etl.iloc[no_t]['dest_table'] + '''
                    where 1=1
                    and ''' + df_active_etl.iloc[no_t]['primary_key_col'] + ''' in ('''+records+''' );
                '''
                print(del_sql)
                dwh_cursor.execute(del_sql)
                field_names = [i[0] for i in live_cursor.description]
                source_df = pd.DataFrame.from_records(updated_records, columns=field_names)
                source_df.to_sql(df_active_etl.iloc[no_t]['dest_table'], schema='public', con=con_to_dwh,
                                 if_exists='append', index=False, method='multi', chunksize=10000)
    def main(self):
        self.full_etl_process()
        self.incremental_etl_process()
        self.update_existing_tables()



if __name__ == '__main__':
    etl = ETL()
    etl.main()
