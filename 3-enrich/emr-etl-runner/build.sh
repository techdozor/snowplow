#!/bin/bash

# Copyright (c) 2015-2017 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Fred Blundun (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2015 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

source ~/.rvm/scripts/rvm

rvm install jruby-9.1.6.0
rvm use --default jruby-9.1.6.0

gem uninstall bundler -a -x
gem install --development bundler

bundle install
echo 'Running RSpec'
rspec
echo 'Runing Rake'

# see snowplow/snowplow#3493
if [ "$CI" = "true" ]; then
  wget https://raw.githubusercontent.com/bundler/bundler/v1.16.0/bundler.gemspec \
    -O /home/travis/.rvm/rubies/jruby-9.1.6.0/lib/ruby/gems/shared/gems/bundler-1.16.0/bundler.gemspec
  ls /home/travis/.rvm/rubies/jruby-9.1.6.0/lib/ruby/gems/shared/gems/bundler-1.16.0/
fi

rake
exit 1