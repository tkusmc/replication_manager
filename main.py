import socket
import database


servers = [
    {
        "prod1": "192.168.1.122",
        "prod2": "192.168.1.123"
    },
    {
        "prod_m1": "192.168.1.122", # prod mirror
        "prod_m2": "192.168.1.122"
    },
    {
        "int1": "172.16.50.122",
        "int2": "172.16.50.122"
    },
    {
        "dev1": "192.168.1.122",
        "dev2": "192.168.1.122"
    },
]



primary_ip      = '192.168.1.122'
secondary_ip    = '192.168.1.123'


def main():
    choice = 0
    working = 'Working...'

    print(menu())

    while not choice == 'q':
        choice = input('Choose: ')

        if choice == '1':
            rep_status()

        elif choice == '2':
            rebuild_subscriptions()

        elif choice == '3':
            create_tables()

        elif choice == '4':
            rebuild_replication()

        elif choice == '5':
            create_roles()

        elif choice == '6':
            create_permissions()

        elif choice == '7':
            insert_data()

        elif choice == '8':
            display_message(working)
            run_e164()

        elif choice == '9':
            run_update()

        elif choice == '10':
            get_local_values()


def menu():
    menu = '- Simplicity Replication Validation and Repair Assistant -\n\n'
    menu += 'Please select from the following list:\n'
    menu += ' 1.  Get Replication Status\n'
    menu += ' 2.  Rebuild Subscriptions\n'
    menu += ' 3.  Check Replication\n'
    menu += ' 4.  Rebuild Replication\n'
    menu += ' 5.  Disable Subscription\n'
    menu += ' 6.  Enable Subscription\n'
    menu += ' 7.  Create/Update Initial Data and Settings\n'
    menu += ' 8.  Create/Update e164 Data\n'
    menu += ' 9.  Update Database (Items 3, 4, 6, 7)\n'
    menu += '10.  Run All (Items 1 - 8)\n'
    menu += 'q to Quit\n'

    return menu


def display_message(msg):
    print(msg)


# function to call all the other functions.
def rep_status():

    print(get_status())


def run_update():





# REPLICATION MENU FUNCTIONS
def get_status():
    # Gets the replication status

    # get db connection
    db_local = database.get_db(primary_ip)

    for i in db_local:
        data = i.execute("""SELECT * FROM replication_check_status() """).fetchall()

        return data


def rebuild_subscriptions():
    # Drop/Create local and remote subscriptions

    # drop subscriptions on primary and secondary servers
    drop_subscriptions()

    # create subscriptions on primary and secondary servers
    create_subscriptions()

    # TODO return status from both servers


def check_replication():
    script = "2-DATABASE simplicity_v2.0_create_db_types.sql"
    result = script + ' completed successfully.'
    source = 'Function: create_types()'

    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as curs:
                curs.execute(open(db_script_path + script, "r").read())
    except Exception as err:
        etype = 'Error'
        event_log(err, etype, source)
    else:
        etype = 'Information'
        event_log(result, etype, source)


def rebuild_replication():
    # Removes replication subs, pubs, & init and rebuilds it.
    # This is handy if you're not sure what's wrong.

    # drop subscriptions on primary and secondary servers
    drop_subscriptions()

    # remove replication
    remove_replication()

    # initialize on both servers
    initialize_replication()   # TODO ADD PARAMETERS

    # create subscriptions on primary and secondary servers
    create_subscriptions()


def disable_subscription():

    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as curs:
                curs.execute(open(db_script_path + script, "r").read())
    except Exception as err:
        etype = 'Error'
        event_log(err, etype, source)
    else:
        etype = 'Information'
        event_log(result, etype, source)


def enable_subscription():

    try:
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as curs:
                curs.execute(open(db_script_path + script, "r").read())
    except Exception as err:
        etype = 'Error'
        event_log(err, etype, source)
    else:
        etype = 'Information'
        event_log(result, etype, source)


# HELPER FUNCTIONS
def drop_subscriptions():
    # get db connection
    db_primary = database.get_db(primary_ip)
    db_secondary = database.get_db(secondary_ip)

    # try:
    for i in db_primary:
        data = i.execute("""SELECT * FROM replication_drop_simplicity_subscriptions() """).fetchall()

    for i in db_secondary:
        data = i.execute("""SELECT * FROM replication_drop_simplicity_subscriptions() """).fetchall()

    return data


def create_subscriptions():
    # get db connection
    db_primary = database.get_db(primary_ip)
    db_secondary = database.get_db(secondary_ip)

    # try:
    for i in db_primary:
        data = i.execute("""SELECT * FROM replication_create_simplicity_subscriptions() """).fetchall()

    for i in db_secondary:
        data = i.execute("""SELECT * FROM replication_create_simplicity_subscriptions() """).fetchall()

    return data


def remove_replication():
    # Make sure subscriptions are dropped first.

    # TODO Ensure there are no subscriptions first

    # get db connection
    db_primary = database.get_db(primary_ip)
    db_secondary = database.get_db(secondary_ip)

    # remove replication
    for i in db_primary:
        data = i.execute("""SELECT * FROM replication_remove() """).fetchall()

    # remove replication
    for i in db_secondary:
        data = i.execute("""SELECT * FROM replication_remove() """).fetchall()


def initialize_replication(in_primary_ip: str, in_secondary_ip: str):
    # TODO figure out input parameters and check system_locals

    # get db connection
    db_primary = database.get_db(primary_ip)
    db_secondary = database.get_db(secondary_ip)

    # remove replication
    for i in db_primary:
        data = i.execute("""SELECT * FROM replication_config_init(%,%) """,
                         (in_primary_ip, in_secondary_ip)).fetchall()

    # remove replication
    for i in db_secondary:
        data = i.execute("""SELECT * FROM replication_config_init() """,
                         (in_primary_ip, in_secondary_ip)).fetchall()


def get_local_values():

    # get local ip


    # get local hostname
    res_host = socket.gethostname()
    res_host, res_alias, res_ip = socket.gethostbyaddr('127.0.0.1')
    print(res_host)



if __name__ == "__main__":
    main()