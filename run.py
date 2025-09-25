# run.py
from app import create_app
from app.cli import register_cli
app = create_app()
register_cli(app)

if __name__ == '__main__':
    app.run(
        debug=bool(__import__('os').environ.get('FLASK_DEBUG') == '1'),
        host='0.0.0.0',
        port=int(__import__('os').environ.get('PORT', 5000))
    )
