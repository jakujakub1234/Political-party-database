def encode(password):
  shift = 14
  cipherText = ""
  for ch in password:
    if ch.isalpha():
      stayInAlphabet = ord(ch) + shift 
      if stayInAlphabet > ord('z'):
        stayInAlphabet -= 26
      finalLetter = chr(stayInAlphabet)
      cipherText += finalLetter
    elif ch.isnumeric():
      cipherText += str((int(ch) + 7) % 10)
    else:
      cipherText += ch
  return cipherText
  
def decode(password):
  shift = 12
  cipherText = ""
  for ch in password:
    if ch.isalpha():
      stayInAlphabet = ord(ch) + shift 
      if stayInAlphabet > ord('z'):
        stayInAlphabet -= 26
      finalLetter = chr(stayInAlphabet)
      cipherText += finalLetter
    elif ch.isnumeric():
      cipherText += str((int(ch) + 3) % 10)
    else:
      cipherText += ch
  return cipherText