import random
import pprint

if __name__ == '__main__':
    workers = [f"w_{idx}" for idx in range(1, 44)]
    geometries = [f"Geometry {idx}" for idx in range(1, 9)]

    human_factor_data = []
    for worker in workers:
        for geometry in geometries:
            entry = {
                "geometry": geometry,
                "preference": round(random.uniform(0.0, 1.0), 2),
                "resilience": round(random.uniform(0.0, 1.0), 2),
                "medical_condition": "true",
                "experience": round(random.uniform(0.0, 1.0), 2),
                "worker": worker,
            }
            human_factor_data.append(entry)

    print(pprint.pformat(human_factor_data))