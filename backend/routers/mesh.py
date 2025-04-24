from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from ..models.database import get_db
from ..models.mesh import Mesh, Tag, MeshCreate, MeshUpdate, MeshResponse, TagResponse
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter(
    prefix="/mesh",
    tags=["mesh"],
    responses={404: {"description": "Not found"}},
)


@router.get("/", response_model=List[MeshResponse])
def get_all_meshes(
    skip: int = 0, 
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get all available 3D meshes"""
    meshes = db.query(Mesh).offset(skip).limit(limit).all()
    return meshes


@router.get("/{mesh_id}", response_model=MeshResponse)
def get_mesh_by_id(mesh_id: int, db: Session = Depends(get_db)):
    """Get a specific 3D mesh by ID"""
    mesh = db.query(Mesh).filter(Mesh.id == mesh_id).first()
    if mesh is None:
        raise HTTPException(status_code=404, detail=f"Mesh with ID {mesh_id} not found")
    return mesh


@router.get("/search/", response_model=List[MeshResponse])
def search_meshes(
    name: Optional[str] = None,
    tag: Optional[str] = None,
    format: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Search for meshes by name, tags, or format"""
    query = db.query(Mesh)
    
    if name:
        query = query.filter(Mesh.name.ilike(f"%{name}%"))
    
    if format:
        query = query.filter(Mesh.format == format)
    
    if tag:
        query = query.join(Mesh.tags).filter(Tag.name == tag)
    
    return query.all()


@router.post("/", response_model=MeshResponse, status_code=201)
def create_mesh(mesh_data: MeshCreate, db: Session = Depends(get_db)):
    """Create a new mesh entry"""
    try:
        # Create new mesh object
        db_mesh = Mesh(
            name=mesh_data.name,
            vertices=mesh_data.vertices,
            faces=mesh_data.faces,
            format=mesh_data.format,
            preview_url=mesh_data.preview_url,
            description=mesh_data.description,
            file_path=mesh_data.file_path
        )
        
        # Handle tags
        for tag_name in mesh_data.tags:
            # Check if tag already exists
            tag = db.query(Tag).filter(Tag.name == tag_name).first()
            if not tag:
                # Create new tag if it doesn't exist
                tag = Tag(name=tag_name)
                db.add(tag)
                db.flush()  # Flush to get the tag ID
            
            # Add tag to mesh
            db_mesh.tags.append(tag)
        
        db.add(db_mesh)
        db.commit()
        db.refresh(db_mesh)
        return db_mesh
    
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.put("/{mesh_id}", response_model=MeshResponse)
def update_mesh(mesh_id: int, mesh_data: MeshUpdate, db: Session = Depends(get_db)):
    """Update an existing mesh entry"""
    db_mesh = db.query(Mesh).filter(Mesh.id == mesh_id).first()
    if db_mesh is None:
        raise HTTPException(status_code=404, detail=f"Mesh with ID {mesh_id} not found")
    
    try:
        # Update mesh attributes if provided
        if mesh_data.name is not None:
            db_mesh.name = mesh_data.name
        if mesh_data.vertices is not None:
            db_mesh.vertices = mesh_data.vertices
        if mesh_data.faces is not None:
            db_mesh.faces = mesh_data.faces
        if mesh_data.format is not None:
            db_mesh.format = mesh_data.format
        if mesh_data.preview_url is not None:
            db_mesh.preview_url = mesh_data.preview_url
        if mesh_data.description is not None:
            db_mesh.description = mesh_data.description
        if mesh_data.file_path is not None:
            db_mesh.file_path = mesh_data.file_path
        
        # Update tags if provided
        if mesh_data.tags is not None:
            # Clear existing tags
            db_mesh.tags = []
            
            # Add new tags
            for tag_name in mesh_data.tags:
                tag = db.query(Tag).filter(Tag.name == tag_name).first()
                if not tag:
                    tag = Tag(name=tag_name)
                    db.add(tag)
                    db.flush()
                db_mesh.tags.append(tag)
        
        db.commit()
        db.refresh(db_mesh)
        return db_mesh
    
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.delete("/{mesh_id}", status_code=204)
def delete_mesh(mesh_id: int, db: Session = Depends(get_db)):
    """Delete a mesh entry"""
    db_mesh = db.query(Mesh).filter(Mesh.id == mesh_id).first()
    if db_mesh is None:
        raise HTTPException(status_code=404, detail=f"Mesh with ID {mesh_id} not found")
    
    try:
        db.delete(db_mesh)
        db.commit()
        return None
    
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")