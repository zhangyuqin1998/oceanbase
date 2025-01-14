/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_service.h"
using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;

int ObTableService::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sess_pool_mgr_.init())) {
    LOG_WARN("fail to init tableapi session pool manager", K(ret));
  }
  return ret;
}

void ObTableService::stop()
{
  sess_pool_mgr_.stop();
}

void ObTableService::wait()
{
  sess_pool_mgr_.wait();
}

void ObTableService::destroy()
{
  sess_pool_mgr_.destroy();
}
