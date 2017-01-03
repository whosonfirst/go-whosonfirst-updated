# go-whosonfirst-updated

Mapzen specific packages for handling updates to Who's On First data. If it works for you too that is also excellent.

![](images/wof-updated-2-arch.png)

_Something like this. It's still wet paint..._

## Important

Too soon. Move along.

## Tools

### wof-updated-replay

For example:

```
./bin/wof-updated-replay -repo /usr/local/data/whosonfirst-data-venue-us-ca --start-commit 613b6e7cf63ae58231a596ffa1b2e80e9f2b9038
2016/12/26 18:52:53 log --pretty=format:#%H --name-only 613b6e7cf63ae58231a596ffa1b2e80e9f2b9038^...HEAD
2016/12/26 18:52:53 044ca5543338d1e3d1788a3d522f42b9cea08517,whosonfirst-data-venue-us-ca,data/110/878/641/1/1108786411.geojson
044ca5543338d1e3d1788a3d522f42b9cea08517,whosonfirst-data-venue-us-ca,data/110/878/641/3/1108786413.geojson
e0653652b33a8f1b473c05f8815131b404b7ffde,whosonfirst-data-venue-us-ca,data/588/389/817/588389817.geojson
e0653652b33a8f1b473c05f8815131b404b7ffde,whosonfirst-data-venue-us-ca,data/588/390/107/588390107.geojson
d357d622e71f41166163566d7882d7e79abc449b,whosonfirst-data-venue-us-ca,data/588/393/401/588393401.geojson
0d112a318c23c3d12a1b4ea251239c7570c46b8a,whosonfirst-data-venue-us-ca,data/588/370/399/588370399.geojson
a00fc56c39bcedbee718ef810956c729b2a5ac7c,whosonfirst-data-venue-us-ca,data/102/460/900/1/1024609001.geojson
a00fc56c39bcedbee718ef810956c729b2a5ac7c,whosonfirst-data-venue-us-ca,data/110/872/348/9/1108723489.geojson
a00fc56c39bcedbee718ef810956c729b2a5ac7c,whosonfirst-data-venue-us-ca,data/110/872/488/9/1108724889.geojson
a00fc56c39bcedbee718ef810956c729b2a5ac7c,whosonfirst-data-venue-us-ca,data/110/872/489/3/1108724893.geojson
a00fc56c39bcedbee718ef810956c729b2a5ac7c,whosonfirst-data-venue-us-ca,data/110/872/490/9/1108724909.geojson
```

And then this happens assuming you've done something like `./bin/wof-updated -data-root /usr/local/data -s3 -es -es-index spelunker -loglevel debug -stdout`:

```
updated 18:52:49.429649 [info] ready to process tasks
updated 18:52:49.429875 [info] ready to process pubsub messages
updated 18:52:49.430781 [info] ready to receive pubsub messages
updated 18:52:53.107670 [info] got task: {044ca5543338d1e3d1788a3d522f42b9cea08517 whosonfirst-data-venue-us-ca [data/110/878/641/1/1108786411.geojson data/110/878/641/3/1108786413.geojson]}
updated 18:52:53.107689 [info] invoking s3
updated 18:52:53.107699 [info] invoking elasticsearch
updated 18:52:53.107709 [info] got task: {e0653652b33a8f1b473c05f8815131b404b7ffde whosonfirst-data-venue-us-ca [data/588/389/817/588389817.geojson data/588/390/107/588390107.geojson]}
updated 18:52:53.107715 [info] invoking s3
updated 18:52:53.107722 [info] invoking elasticsearch
updated 18:52:53.107880 [debug] /usr/local/bin/wof-es-index-filelist --host localhost --port 9200 --index spelunker /tmp/updated475727395
updated 18:52:53.110085 [info] got task: {d357d622e71f41166163566d7882d7e79abc449b whosonfirst-data-venue-us-ca [data/588/393/401/588393401.geojson]}
updated 18:52:53.110099 [info] invoking s3
updated 18:52:53.110112 [info] invoking elasticsearch
updated 18:52:53.110125 [info] got task: {0d112a318c23c3d12a1b4ea251239c7570c46b8a whosonfirst-data-venue-us-ca [data/588/370/399/588370399.geojson]}
updated 18:52:53.110132 [info] invoking s3
updated 18:52:53.110139 [info] invoking elasticsearch
updated 18:52:53.110197 [info] got task: {a00fc56c39bcedbee718ef810956c729b2a5ac7c whosonfirst-data-venue-us-ca [data/102/460/900/1/1024609001.geojson data/110/872/348/9/1108723489.geojson data/110/872/488/9/1108724889.geojson data/110/872/489/3/1108724893.geojson data/110/872/490/9/1108724909.geojson]}
updated 18:52:53.110219 [info] invoking s3
updated 18:52:53.110232 [info] invoking elasticsearch
... and so on
```

## See also

* https://github.com/whosonfirst/go-webhookd
