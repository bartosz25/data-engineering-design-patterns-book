from output_generation_factory import OutputType, OutputGenerationFactory

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(prog='Delta Lake or CSV output generation')
    parser.add_argument('--input_dir', required=True)
    parser.add_argument('--output_dir', required=True)
    parser.add_argument('--output_type', required=True, type=OutputType)
    args = parser.parse_args()

    output_generation_factory = OutputGenerationFactory(args.output_type)

    spark_session = output_generation_factory.get_spark_session()

    raw_data = (spark_session.read.schema('type STRING, full_name STRING, version STRING').format('json')
                .load(args.input_dir))

    output_generation_factory.write_devices_data(raw_data, args.output_dir)
