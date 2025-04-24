from .database import engine, Base
from .mesh import Mesh, Tag, mesh_tag_association

def init_db():
    """Initialize the database tables"""
    Base.metadata.create_all(bind=engine)

if __name__ == "__main__":
    print("Creating database tables...")
    init_db()
    print("Database tables created successfully!")