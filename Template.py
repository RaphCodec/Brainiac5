from datetime import datetime
import json

def main():
    print('hi')


if __name__ == '__main__':
    start = datetime.now()
    print( f'\nScript starts:  { start  }' )


    config = "./Template.config.json"
    
    with open(config, 'r') as json_file:
        json_config = json.load(json_file)
    DEST_SQL_SERVER             = json_config['DEST_SQL_SERVER']
    DEST_SQL_DATABASE           = json_config['DEST_SQL_DATABASE']
    DEST_SQL_TABLE              = json_config['DEST_SQL_TABLE']

    LOG_FILE_PATH               = json_config['LOG_FILE_PATH']

    try:
        from pprint import pformat

        print( f"Using the following config:\n\n{ pformat(json_config, sort_dicts=False) }" )
        print( f'{"*" * 40}\n' )

        main()

        end = datetime.now()
        print( f'\n{"*" * 40}' )
        print( f'Script ended:  {end}' )

    except Exception as e:
        print( f"ERROR: '{e}'\n" )

        end = datetime.now()
        print( '\n*******************************************************' )
        print( f'Script ended:  {end}' )
       