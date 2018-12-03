''' Creates an application instance and runs the server '''


if __name__ == '__main__':
    from datatokafka.application import create_app
    app = create_app()
    app.run(host='0.0.0.0', port=11181)
