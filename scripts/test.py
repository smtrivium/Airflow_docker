
import clickhouse_connect

create_table_sql = """
CREATE TABLE BP_clk.spr_PnL1
(
    `id` Int32,
    `account_name` String,
    `parent_id` Int32,
    `hierarchy` Int32,
    `beginning_day` DateTime('UTC'),
    `ending_day` DateTime('UTC')
   
)
ENGINE = MergeTree
PRIMARY KEY(`id`)

"""

def create_table(sql):
    client = clickhouse_connect.get_client(host='localhost', port='8122', user='default', password= '')
    client.command('DROP TABLE IF EXISTS BP_clk.spr_PnL1')
    client.command(sql)
    print('table has been created')


a="s"
def cr():
    print('ok'+ a)
    #create_table(create_table_sql)
