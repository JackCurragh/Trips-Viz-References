'''
Script to merge two or more trips sqlites into one SQLite.

Input:
    -i, --input: Input SQLite files to merge. Can be multiple files.
    -o, --output: Output SQLite file.


Usage:
    python merge.py -i input1.sqlite input2.sqlite -o output.sqlite
'''

import argparse
import sqlite3
import sqlitedict


def merge_dict(dict1, dict2):
    '''
    Merge two dictionaries.
    '''
    
    for key in dict2:
        if type(dict2[key]) == dict:
            dict1[key] = merge_dict(dict1[key], dict2[key])
        else:
            if key in dict1:
                dict1[key] += dict2[key]
            else:
                dict1[key] = dict2[key]

    return dict1


def process_transcript(output_transcript_dict, transcirpt_dict):
    '''
    Process two transcripts and merge them into one.
    '''
    for data_type in transcirpt_dict:
        if data_type in output_transcript_dict:
            for length in transcirpt_dict[data_type]:
                if length not in output_transcript_dict[data_type]:
                    output_transcript_dict[data_type][length] = transcirpt_dict[data_type][length]
                else:
                    output_transcript_dict[data_type][length] = merge_dict(output_transcript_dict[data_type][length], transcirpt_dict[data_type][length])

        else:
            output_transcript_dict[data_type] = transcirpt_dict[data_type]

    return output_transcript_dict


def merge(input_files: list, output_file: str, transcript_prefix="ENST") -> str:
    '''
    Take list of input sqlite files and merge them into one output file.

    Args:
        input_files: List of input sqlite files.
        output_file: Output sqlite file.

    Returns:
        Output sqlite file.
    '''
    # Connect to output sqlitedict file
    output_file = sqlitedict.SqliteDict(output_file, autocommit=True)

    # Loop through input files
    for input_file in input_files:
        # Connect to input file
        input_conn = sqlitedict.SqliteDict(input_file, autocommit=True)

        # Loop through keys in input file
        for i, key in enumerate(input_conn.keys()):

            if i % 1000 == 0:
                print("###############################################################################################################")
                print(i/len(input_conn)*100, "%")

            if key in output_file:
                if key.startswith(transcript_prefix):
                    output_file[key] = process_transcript(output_file[key], input_conn[key])

                else:
                    if type(input_conn[key]) == dict:
                        for element in input_conn[key]:
                            if element not in output_file[key]:
                                print("Not a transcript and is a dict and not in outputfile:", key, element, type(input_conn[key][element]))
                                if type(input_conn[key][element]) == dict:
                                    output_file[key][element] = input_conn[key][element]
                                elif type(input_conn[key][element]) == list:
                                    output_file[key][element] = input_conn[key][element]
                                else:
                                    print("Alert New: ", key, element, type(input_conn[key][element]))

                            else:
                                if type(input_conn[key][element]) == dict:
                                    output_file[key][element] = merge_dict(output_file[key][element], input_conn[key][element])

                                elif type(input_conn[key][element]) == list:
                                    '''
                                    Note totals[transcript] is a list of 3 elements
                                    I believe these refer to the 3 frames. I believe the values should be summed
                                    This is not happening with the below implementation
                                    '''
                                    print("merging lists: ", key, element, input_conn[key][element], output_file[key][element])
                                    output_file[key][element] = [sum(x) for x in zip(output_file[key][element], input_conn[key][element])]
                                    print("merged list: ", key, element, input_conn[key][element], output_file[key][element])

                                else:
                                    print("Alert Old: ", key, element, type(input_conn[key][element]))
                    else:
                        if type(input_conn[key]) == int:
                            output_file[key] += input_conn[key]
                        else:
                            print("In output file, not a transcript and not a dict: ", key, input_conn[key], type(input_conn[key]))


            if not key.startswith(transcript_prefix):
                print("Not a transcript: ", key)

            # Get value from input file
            value = input_conn[key]

            # Insert value into output file
            output_file[key] = value
        
        # Close input file
        input_conn.close()


    # Close output file
    output_file.close()

    return output_file

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', nargs='+', help='Input SQLite files to merge. Can be multiple files.')
    parser.add_argument('-o', '--output', help='Output SQLite file.')
    args = parser.parse_args()

    merge(args.input, args.output)