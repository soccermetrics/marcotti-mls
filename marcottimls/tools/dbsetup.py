import os
import pkg_resources

import jinja2
from clint.textui import colored, prompt, puts, validators


db_ports = {
    'postgresql': '5432',
    'mysql': '3306',
    'mssql': '1433',
    'oracle': '1521',
    'firebird': '3050'
}

dialect_options = [{'selector': '1', 'prompt': 'PostgreSQL', 'return': 'postgresql'},
                   {'selector': '2', 'prompt': 'MySQL', 'return': 'mysql'},
                   {'selector': '3', 'prompt': 'SQL Server', 'return': 'mssql'},
                   {'selector': '4', 'prompt': 'Oracle', 'return': 'oracle'},
                   {'selector': '5', 'prompt': 'Firebird', 'return': 'firebird'},
                   {'selector': '6', 'prompt': 'SQLite', 'return': 'sqlite'}]


def path_query(query_string):
    path_txt = prompt.query(query_string, validators=[])
    return None if path_txt == '' else os.path.split(path_txt)


def setup_user_input():
    """
    Setup configuration and database loading script by querying information from user.

    :return:
    """
    print "#### Please answer the following questions to setup the folder ####"
    work_folder = prompt.query('Work folder (must exist):', default='.', validators=[validators.PathValidator()])
    log_folder = prompt.query('Logging folder (must exist):', default='.', validators=[validators.PathValidator()])
    config_file = prompt.query('Config file name:', default='local')
    config_class = prompt.query('Config class name:', default='LocalConfig')
    print "#### Database configuration setup ####"
    dialect = prompt.options('Marcotti-MLS Database backend:', dialect_options)
    if dialect == 'sqlite':
        dbname = prompt.query('Database filename:', validators=[validators.FileValidator()])
        dbuser = ''
        hostname = ''
        dbport = 0
    else:
        dbname = prompt.query('Database name:')
        dbuser = prompt.query('Database user:', default='')
        puts(colored.red('Database password is not defined -- You must define it in the config file!'))
        hostname = prompt.query('Database hostname:', default='localhost')
        dbport = prompt.query('Database path:', default=db_ports.get(dialect))
    print "#### Database season setup ####"
    start_yr = prompt.query('Start season year', default='1990', validators=[validators.IntegerValidator()])
    end_yr = prompt.query('End season year', default='2020', validators=[validators.IntegerValidator()])
    print "#### Data file setup ####"
    top_level_data_dir = prompt.query('Directory containing CSV data files:', default='.',
                                      validators=[validators.PathValidator()])
    club_data_path = path_query('Relative path of Clubs data files:')
    comp_data_path = path_query('Relative path of Competitions data files:')
    comp_season_data_path = path_query('Relative path of CompetitionSeasons data files:')
    player_data_path = path_query('Relative path of Players data files:')
    acq_data_path = path_query('Relative path of Player Acquisitions data files:')
    salary_data_path = path_query('Relative path of Salaries data files:')
    partial_data_path = path_query('Relative path of Partial Tenure data files:')
    minutes_data_path = path_query('Relative path of Player Minutes data files:')
    field_stat_data_path = path_query('Relative path of Field Player Statistics data files:')
    gk_stat_data_path = path_query('Relative path of Goalkeeper Statistics data files:')
    points_data_path = path_query('Relative path of League Points data files:')
    print "#### End setup questions ####"

    setup_dict = {
        'config_file': config_file.lower(),
        'config_class': config_class,
        'dialect': dialect,
        'dbname': dbname,
        'dbuser': dbuser,
        'dbhost': hostname,
        'dbport': dbport,
        'start_yr': start_yr,
        'end_yr': end_yr,
        'logging_dir': log_folder,
        'log_file_path': os.path.join(log_folder, 'marcotti.log'),
        'data_dir': top_level_data_dir,
        'data': {
            'clubs': club_data_path,
            'competitions': comp_data_path,
            'comp_seasons': comp_season_data_path,
            'players': player_data_path,
            'acquisitions': acq_data_path,
            'salaries': salary_data_path,
            'partials': partial_data_path,
            'minutes': minutes_data_path,
            'field_stats': field_stat_data_path,
            'gk_stats': gk_stat_data_path,
            'points': points_data_path
        }
    }
    return work_folder, setup_dict


def main():
    DATA_PATH = pkg_resources.resource_filename('marcottimls', 'data/')
    main_folder, setup_dict = setup_user_input()
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(searchpath=DATA_PATH),
                             trim_blocks=True, lstrip_blocks=True)
    template_files = ['local.skel', 'logging.skel', 'loader.skel']
    output_files = ['{}.py'.format(setup_dict['config_file']), 'logging.json', 'loader.py']

    for template_file, output_file in zip(template_files, output_files):
        template = env.get_template(os.path.join('templates', template_file))
        with open(os.path.join(main_folder, output_file), 'w') as g:
            result = template.render(setup_dict)
            g.write(result)
            print "Configured {}".format(os.path.join(main_folder, output_file))
    print "#### Setup complete ###"
