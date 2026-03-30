"""
TeleDrive Route Blueprints.
Split from web_app.py for maintainability.
"""
from .auth_routes import auth_bp
from .admin_routes import admin_bp
from .profile_routes import profile_bp
from .channel_routes import channel_bp
from .scraper_routes import scraper_bp
from .settings_routes import settings_bp
from .google_routes import google_bp


def register_blueprints(app):
    """Register all blueprints with the Flask app."""
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(profile_bp)
    app.register_blueprint(channel_bp)
    app.register_blueprint(scraper_bp)
    app.register_blueprint(settings_bp)
    app.register_blueprint(google_bp)
