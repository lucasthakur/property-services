package attom

import (
	"regexp"
)

var photoSizePattern = regexp.MustCompile(`-w\d+_h\d+`)

func upgradePhotoURL(href string) string {
	if href == "" {
		return href
	}
	return photoSizePattern.ReplaceAllString(href, "-w2048_h1536")
}
