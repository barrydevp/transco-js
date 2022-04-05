exports._delay = (ms) => new Promise((rs) => {
  setTimeout(rs, ms)
})

exports.ErrorWrapRetryable = (err) => {
  if (!err) {
    return err
  }
  err.__retryable = true
  return err
}

exports.IsRetryableErr = (err) => {
  return err && err.__retryable
}

exports.ParseUri = (uri) => {
  const regex = /^(\w+):\/\/([a-zA-Z0-9,:]+)(.*)/gi
  const parsed = regex.exec(uri)
  if (!parsed) {
    throw new Error('Invalid URI')
  }

  return {
    protocol: parsed[1] + ':',
    scheme: parsed[1],
    host: parsed[2],
    rawPath: parsed[3],
  }
}
