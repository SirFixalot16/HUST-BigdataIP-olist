from airflow.www.app import create_app
from airflow.providers.fab.auth_manager.models import User

app = create_app()
with app.app_context():
    sm = app.appbuilder.sm
    role_admin = sm.find_role("Admin")
    
    # Kiểm tra và tạo user
    user = sm.add_user(
        username="binh_admin",
        firstname="Binh",
        lastname="Nguyen",
        email="binh@example.com",
        role=role_admin,
        password="YourSecretPassword123"
    )
    if user:
        print("User 'binh_admin' created successfully.")
    else:
        print("Failed to create user or user already exists.")