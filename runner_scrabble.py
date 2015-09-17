# Using the output of an MR job in
# a regular python program

from scrabble import MRScrabble

mr_job = MRScrabble(args=['sowpods.txt'])

with mr_job.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        key, value = mr_job.parse_output_line(line)
        value.sort(reverse=True)
        for s, w  in value:
            print( "%3d %s" % (s, w ))
