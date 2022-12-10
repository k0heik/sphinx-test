import yaml
from jinja2 import Environment, FileSystemLoader


def generate(functional_name, cfg):
    if cfg["type"] ==  "Batch":
        template_name = "serverless_batch.tpl.yml"
    elif cfg["type"] ==  "SimpleLambda":
        template_name = "serverless_simplelambda.tpl.yml"
    else:
        raise ValueError("Invalid type")

    env = Environment(loader=FileSystemLoader("../commons", encoding="utf8"))
    tpl = env.get_template(template_name)

    output = tpl.render(**cfg)

    if functional_name == "default":
        filename = f"serverless.yml"
    else:
        filename = f"serverless_{functional_name}.yml"

    open(filename, "w").write(output)


if __name__ == "__main__":
    with open('serverless_config.yml') as file:
        cfgs = yaml.safe_load(file)

    for functional_name, cfg in cfgs["system_functions"].items():
        generate(functional_name, cfg)
