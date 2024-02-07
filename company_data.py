import assets.helper as b3
import assets.functions as run
def load_system(value, rebase=False):
     
    # # load df_nsd
	# df_nsd = load_nsd()
    df_nsd = run.load_parquet('nsd')
    print('fast debug df_nsd')

	# load df_rad
    df_rad = run.load_rad(df_nsd)
	# df_rad = load_parquet('rad')
	# print('fast debug df_rad')

    return value



def periodic_task():
    # BASE GROUND UPDATE
    value = 'load companies'
    value = load_system(value)

    return value

import datetime
for i in range(100):
    try:
        if __name__ == "__main__":
            value = periodic_task()  

            print(value)
            print(i, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e)
    except Exception as e:
        print(i, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e)
        pass


print('done')