from processorpipeline import UsageStatsProcessorPipeline
from configcontext import ConfigurationContext
import time
import argparse
import datetime
from calendar import monthrange
import stages
import sys

import pymysql
pymysql.install_as_MySQLdb()

def print_identifier_replacements_table(replacements):
    print("")
    print("Identifier normalization dry-run (original -> reemplazo)")
    print("original_identifier\treplaced_identifier\tchanged")
    for item in replacements:
        changed = "yes" if item.get("changed") else "no"
        print(
            f"{item.get('original_identifier', '')}\t"
            f"{item.get('replaced_identifier', '')}\t{changed}"
        )


def handle_identifier_test_mode(result):
    replacements = getattr(result, "identifier_replacements", None) or []
    total = len(replacements)
    changed = sum(1 for item in replacements if item.get("changed"))

    print(f"Identifiers processed: {total}")
    print(f"Identifiers changed: {changed}")
    print(f"Identifiers unchanged: {total - changed}")

    if total == 0:
        print("No se encontraron documentos para procesar en la partición seleccionada.")
        return 2

    print_identifier_replacements_table(replacements)
    return 0


def main(args, parser):
   
    
    config_context = ConfigurationContext(args)
    is_identifier_test = args.get("test") == "identifiers"
    output_stage = "stages.NoOpOutputStage" if is_identifier_test else "stages.ElasticOutputStage"
    
    try:
        pipeline = UsageStatsProcessorPipeline(config_context, 
                                       "stages.S3ParquetInputStage",
                                        
                                       ["stages.RobotsFilterStage",
                                        "stages.AssetsFilterStage",
                                        "stages.MetricsFilterStage",
                                        "stages.AggByItemFilterStage",
                                        "stages.IdentifierFilterStage",
                                       ],
                                       
                                        output_stage)
        result = pipeline.run()
        
    except Exception as e:
        print("Error: %s" % e)
        ## print trace
        import traceback
        traceback.print_exc()
        return 1

    if result is None:
        print("Pipeline execution failed. Review traceback and OpenSearch connectivity/settings.")
        return 1

    if is_identifier_test:
        return handle_identifier_test_mode(result)

    documents_count = 0
    if result is not None and getattr(result, "documents", None) is not None:
        documents_count = len(result.documents)

    if documents_count == 0:
        print("No se encontraron documentos para indexar con los parámetros indicados.")
        return 2

    return 0
        
    

def build_parser():
    parser = argparse.ArgumentParser(
        description="Usage Stats Processor (S3 parquet to OpenSearch)",
        epilog="Example: python s3parquet2elastic.py -c config.ini -s 48 -y 2025 -m 1 -t R",
    )

    parser.add_argument( "-c", "--config_file_path", default='config.ini', help="config file", required=False )
    parser.add_argument( "-s", "--site", type=int, help="site id", required=True)
   
    parser.add_argument( "-y", "--year", type=int, help="yyyy", required=True )
    parser.add_argument("-m", "--month", type=int, help="m", required=True)
    parser.add_argument("-d", "--day", default=None, type=int, help="d", required=False)

    parser.add_argument("-t",
                    "--type", 
                    default='R', 
                    type=str, 
                    help="(R|L|N)", 
                    required=False)

    parser.add_argument(
        "--test",
        choices=["identifiers"],
        default=None,
        help="run a dry-run test mode without indexing (e.g. --test identifiers)",
        required=False,
    )
    return parser


def validate_args(parser, args):
    if args.site <= 0:
        parser.error("argument -s/--site: must be a positive integer")

    if args.year < 1 or args.year > 9999:
        parser.error("argument -y/--year: must be between 1 and 9999")

    if args.month < 1 or args.month > 12:
        parser.error("argument -m/--month: must be between 1 and 12")

    if args.day is not None:
        if args.day < 1:
            parser.error("argument -d/--day: must be greater than or equal to 1")
        max_day = monthrange(args.year, args.month)[1]
        if args.day > max_day:
            parser.error(
                f"argument -d/--day: must be between 1 and {max_day} for {args.year}-{args.month:02d}"
            )

    args.type = args.type.upper()
    if args.type not in {"R", "L", "N"}:
        parser.error("argument -t/--type: must be one of R, L, N")


def parse_args(parser):
    args = parser.parse_args()
    validate_args(parser, args)
    return args
    

if __name__ == "__main__":

    start_time = time.time()

    parser = build_parser()

    # parse arguments
    args = vars(parse_args(parser))
    print("Arguments: ", args )     
    
    # run the main function
    exit_code = main(args, parser)

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Tiempo de ejecución: {elapsed_time} segundos")
    sys.exit(exit_code)
