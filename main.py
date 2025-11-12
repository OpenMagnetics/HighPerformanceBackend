from fastapi import FastAPI, Request, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import sys
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String
import sqlalchemy
import datetime
import time
import hashlib
import pathlib
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound
import PyMKF
from timeit import default_timer as timer

from Ansyas import ansyas


app = FastAPI()
temp_folder = pathlib.Path.home() / "temp"
pathlib.Path(temp_folder).mkdir(parents=True, exist_ok=True)
# print("Loading history")
# history_path = f"{pathlib.Path(__file__).parent.resolve()}/app/data/magnetics.ndjson"
# results = PyMKF.load_magnetics_from_file(history_path, True)
# print(f"History loaded: {results} designs")


origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        blobData = file.read()
    return blobData


def writeTofile(data, filename):
    # Convert binary data to proper format and write it on Hard Disk
    with open(filename, 'wb') as file:
        file.write(data)
    print("Stored blob data into: ", filename, "\n")


class AnsyasCacheTable:
    def __init__(self, solution_type):
        self.solution_type = solution_type

    def disconnect(self):
        self.session.close()

    def connect(self):
        self.engine = sqlalchemy.create_engine(f"sqlite:///{temp_folder}\\cache_{self.solution_type}.db", isolation_level="AUTOCOMMIT")

        Base = declarative_base()

        class AnsyasCache(Base):
            __tablename__ = 'ansyas_cache'
            hash = Column(String, primary_key=True)
            data = Column(String)
            created_at = Column(String)

        # Create all tables in the engine
        Base.metadata.create_all(self.engine)

        metadata = sqlalchemy.MetaData()
        metadata.reflect(self.engine, )
        Base = automap_base(metadata=metadata)
        Base.prepare()

        Session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.session = Session()
        self.Table = Base.classes.ansyas_cache

    def insert(self, hash, data):
        try:
            self.connect()
        except sqlalchemy.exc.OperationalError:
            return False
        data = {
            'hash': hash,
            'data': data,
            'created_at': datetime.datetime.now(),
        }
        row = self.Table(**data)
        self.session.add(row)
        self.session.flush()
        self.session.commit()
        self.disconnect()
        return True

    def read(self, hash):
        try:
            self.connect()
        except sqlalchemy.exc.OperationalError:
            return None
        query = self.session.query(self.Table).filter(self.Table.hash == hash)
        try:
            data = query.one().data
        except MultipleResultsFound:
            data = None
        except NoResultFound:
            data = None
        self.disconnect()
        return data


@app.get("/")
async def root():
    return {"message": "Welcome to OpenMagnetics' High Performance API!"}


@app.post("/create_simulation_from_mas", include_in_schema=False)
async def create_magnetic_simulationion_from_mas(request: Request):
    print("Mierda 0")
    json = await request.json()

    mas = json["mas"]
    mas = PyMKF.mas_autocomplete(mas, {})
    operating_point_index = 0
    solution_type = "EddyCurrent"
    outputs_folder = temp_folder
    project_name = f"Unnamed_design_{time.time()}"
    configuration = {
        "number_segments_arcs": 12,
        "initial_mesh_configuration": 2,
        "maximum_error_percent": 5,
        "refinement_percent": 5,
        "scale": 1,
    }

    if "operating_point_index" in json:
        operating_point_index = int(json["operating_point_index"])

    if "configuration" in json:
        configuration = json["configuration"]

    if "solution_type" in json:
        solution_type = json["solution_type"]

    if "project_name" in json:
        project_name = json["project_name"] + f"_{time.time()}"

    hash_value = hashlib.sha256(str(mas).encode()).hexdigest()

    cache = AnsyasCacheTable(solution_type)
    cache.connect()

    cached_datum = cache.read(hash_value)
    if cached_datum is not None:
        print("Hit in cache!")
        return Response(content=cached_datum, media_type="binary/octet-stream")

    ansyas_obj = ansyas.Ansyas(**configuration)

    project = ansyas_obj.create_project(
        outputs_folder=outputs_folder,
        project_name=project_name,
        # specified_version="2023.2",
        non_graphical=False,
        solution_type=solution_type,
        new_desktop_session=False
    )
    ansyas_obj.set_units("meter")
    ansyas_obj.create_magnetic_simulation(
        mas=mas,
        simulate=False,
        operating_point_index=operating_point_index
    )

    output_project_path = ansyas_obj.get_project_location()

    if output_project_path is None:
        raise HTTPException(status_code=418, detail="Wrong data")
    else:
        blob = convertToBinaryData(output_project_path)
        cache.insert(hash_value, blob)

        return Response(content=blob, media_type="binary/octet-stream")


@app.post("/calculate_advised_magnetics", include_in_schema=False)
async def calculate_advised_magnetics(request: Request):
    data = await request.json()
    inputs = data["inputs"]
    maximum_number_results = data["maximum_number_results"]
    filter_flow = data["filter_flow"]

    try:
        print("Starting filtering in local")
        start = timer()
        filter_result = PyMKF.calculate_advised_magnetics_from_cache(inputs, filter_flow, maximum_number_results)
        end = timer()
        print(end - start)
        return filter_result
    except Exception as e:
        print(e)
        raise HTTPException(status_code=418, detail="Error filtering")


@app.post("/remote_available", include_in_schema=False)
async def remote_available(request: Request):
    return True
