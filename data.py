import assets.helper as b3
import assets.functions as run

def periodic_task():
    # BASE GROUND UPDATE
    value = 'load companies'
    value = run.load_system(value)

    return value
try:
    if __name__ == "__main__":
        value = periodic_task()  

        print(value)
except Exception as e:
    print(e)
    pass

print('done')