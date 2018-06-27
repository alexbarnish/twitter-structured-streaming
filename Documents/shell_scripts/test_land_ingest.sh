#!/bin/bash
#!/bin/sh
### Automates NDM-S data prep
### output used in NDMS REST API, https://ndms.geo.apple.com/


### VARIABLES 
today=`date '+%Y%m%d__%H%M%S'`
version="v32"
# local
download_local=/Downloads/
# git
git_folder=/git/WILC/scripts/py/OSM_ingest/land/inputs/
# remote
ingest_hdfs=/ingest/dev/osm_water/



### MAIN
mkdir ~${git_folder}${version} && chmod 777 ~${git_folder}${version}
mkdir ~${git_folder}${version}/land && chmod 777 ~${git_folder}${version}/land/
mkdir ~${git_folder}${version}/land_plus && chmod 777 ~${git_folder}${version}/land_plus/
find ~${download_local} -name '*_land_island_*.csv' -mindepth 1 -maxdepth 1 -mtime -1 -exec cp {} ~${git_folder}${version}/land/ ';'
find ~${download_local} -name '*.csv' -mindepth 1 -maxdepth 1 -mtime -1 -exec cp {} ~${git_folder}${version}/land_plus/ ';'

# git 
cd ${git_folder}${version}
git add .
git commit -m 'Adding inputs for ${version}'
git push


# basemap staging
ssh mr11d01ls-geo05111901.geo.apple.com "cd ~${git_folder}"
ssh mr11d01ls-geo05111901.geo.apple.com "git pull upstream master"
ssh mr11d01ls-geo05111901.geo.apple.com "git fetch upstream && git reset --hard upstream/master"
ssh mr11d01ls-geo05111901.geo.apple.com "hadoop fs -mkdir ${ingest_hdfs}${version} && hadoop fs -chmod 777 ${ingest_hdfs}${version}"
echo "Maybe failing here"
ssh mr11d01ls-geo05111901.geo.apple.com "cd ${rgit_folder}${version} && \
for folder in $(ls); do hdfs dfs -put '$folder' ${ingest_hdfs}${version}; done"
ssh mr11d01ls-geo05111901.geo.apple.com "hostname;date"