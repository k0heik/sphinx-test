import sys
sys.path.append("../integration_test/")

from jinja2 import Environment, FileSystemLoader
from build_targets import BUILD_TARGETS


def main(sub_systems):
    env = Environment(loader=FileSystemLoader("./", encoding="utf8"))
    tpl = env.get_template("ci.tpl.yml")

    notify_needs = sub_systems + ["run_it"]
    it_needs = [f"build_{x}" for x in BUILD_TARGETS]
    it_needs.append("it_prepare_testdata")
    output = tpl.render(
        sub_systems=sub_systems,
        notify_needs=notify_needs,
        it_build_targets=BUILD_TARGETS,
        it_needs=it_needs,
    )
    with open("../.github/workflows/ci.yml", "w") as f:
        f.write(output)


if __name__ == "__main__":
    sub_systems = [
        "cpc_prediction",
        "cvr_prediction",
        "spa_prediction",
        "common_module",
        "sophia-ai",
        "main",
        "record_to_bq",
        "integration_test",
    ]
    main(sub_systems)
