from database_populator import DatabasePopulator

if __name__ == '__main__':
    try:
        DatabasePopulator().run()
    except Exception:
        print('Could not run: Make sure database settings are correct at ./settings.py')
